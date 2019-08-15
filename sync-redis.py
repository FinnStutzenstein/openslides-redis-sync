import argparse
import asyncio
import hashlib
import logging
import sys

import aioredis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
loop = asyncio.get_event_loop()


async def main(production_redis_address, read_only_redis_address):
    logging.info("Production redis: host={} port={}".format(*production_redis_address))
    logging.info("Read only redis:  host={} port={}".format(*read_only_redis_address))

    production_redis = None
    ro_redis = None
    try:
        production_redis = await aioredis.create_redis(
            production_redis_address, loop=loop
        )
        ro_redis = await aioredis.create_redis(read_only_redis_address, loop=loop)

        # Sync them now.
        synchronizer = Synchronizer(production_redis, ro_redis)
        await synchronizer.sync_redis()
    except Exception as e:
        logging.critical("An exception occurred: {}".format(e))
    finally:
        if production_redis:
            production_redis.close()
            await production_redis.wait_closed()
        if ro_redis:
            ro_redis.close()
            await ro_redis.wait_closed()


class Synchronizer:
    change_id_key = "element_cache_change_id"
    full_data_key = "element_cache_full_data"
    schema_key = "element_cache_schema"
    production_marker_key = "element_cache_production_marker"

    def __init__(self, production_redis, ro_redis):
        self.production_redis = production_redis
        self.ro_redis = ro_redis

    async def sync_redis(self):
        # 0) Check, if the production redis is the correct one. The production worker
        #    should write the special key (see self.production_marker_key) into the redis,
        #    so this script can verify, that it is reading from the correct redis.
        if not bool(await self.production_redis.get(self.production_marker_key)):
            logging.critical(
                "The production redis DOES NOT have the marker key '{}'. Are you sure to read from the correct redis instance? Abording...".format(
                    self.production_marker_key
                )
            )
            return

        logging.info("\nStart syncing...")

        # 1) Get important values from ro_redis:
        #   - current change id (or -inf if not there)
        #   - lowest change id (or None if not existent)
        #   - schema version (in tree variables; all None if not existent)
        await self.fetch_ro_redis_data()

        # 2) Get some "unchangeable" values from production_redis:
        #   - lowest change id
        #   - schema version
        self.lowest_change_id_prod = await self.production_redis.zscore(
            self.change_id_key, "_config:lowest_change_id"
        )
        self.schema_production = await self.production_redis.hgetall(self.schema_key)

        # 3) Do a full update if
        #   - any lowest change id is None
        #   - the lowest change ids are not equal
        #   - any schema version is None
        #   - the schema versions are not equal
        #
        #   A full update means to copy full_data, change_id and schema version
        if self.check_full_update():
            await self.do_full_update()

        # 4) Do a partial update (fetch data atomically -> LUA):
        #   - get all change_id entries between the current_change_id+1 and +inf
        #   - get all changed/deleted elements from full_data
        #   - write both into the ro_redis
        else:
            await self.do_partial_update()

        # DO 1) again, just to print the current values for ro_redis
        logging.info("Current RO data:")
        await self.fetch_ro_redis_data()

        logging.info("Done syncing!")

    async def fetch_ro_redis_data(self):
        change_id_ro = await self.ro_redis.zrevrangebyscore(
            self.change_id_key, withscores=True, count=1, offset=0
        )
        if len(change_id_ro) == 0:  # No changes yet
            self.change_id_ro = "-inf"
            self.lowest_change_id_ro = None  # invalid value
        else:
            self.change_id_ro = change_id_ro[0][1]
            self.lowest_change_id_ro = await self.ro_redis.zscore(
                self.change_id_key, "_config:lowest_change_id"
            )
        # schema version
        self.schema_ro = await self.ro_redis.hgetall(self.schema_key)
        _schema_ro = {
            key.decode(): value.decode() for key, value in self.schema_ro.items()
        }
        logging.info(
            """RO: lowest change id:  {}
    current change id: {}
    db schema: {}
    config schema: {}
    migration schema: {}""".format(
                self.lowest_change_id_ro,
                self.change_id_ro,
                _schema_ro.get("db"),
                _schema_ro.get("config"),
                _schema_ro.get("migration"),
            )
        )

    def check_full_update(self):
        if self.lowest_change_id_ro is None:
            logging.info("RO does not have any changes")
            return True
        if self.lowest_change_id_prod is None:
            logging.info("Prod does not have any changes")
            return True
        if self.lowest_change_id_ro != self.lowest_change_id_prod:
            logging.info("Lowest change id is different")
            return True
        if (
            self.schema_ro.get("db") != self.schema_production.get("db")
            or self.schema_ro.get("config") != self.schema_production.get("config")
            or self.schema_ro.get("migration")
            != self.schema_production.get("migration")
        ):
            logging.info("schema changed")
            return True
        return False

    async def do_full_update(self):
        logging.info("Do full update...")
        # Fetch data atomically
        script = """
        local full_data = redis.call('hgetall', KEYS[1])
        local change_id = redis.call('zrangebyscore', KEYS[2], '-inf', '+inf', 'WITHSCORES')
        local schema = redis.call('hgetall', KEYS[3])
        local result = {}
        table.insert(result, full_data)
        table.insert(result, change_id)
        table.insert(result, schema)
        return result
        """
        full_data, change_id, schema = await self.eval_production_redis(
            script, keys=[self.full_data_key, self.change_id_key, self.schema_key]
        )

        self.convert_change_id_list(change_id)

        # Reset RO
        transaction = self.ro_redis.multi_exec()
        transaction.delete(self.full_data_key)
        transaction.delete(self.change_id_key)
        transaction.delete(self.schema_key)

        # Write data
        if full_data:
            transaction.hmset(self.full_data_key, *full_data)
        if change_id:
            transaction.zadd(self.change_id_key, *change_id)
        if schema:
            transaction.hmset(self.schema_key, *schema)
        await transaction.execute()
        logging.info("Full update done!")

    async def do_partial_update(self):
        logging.info("Do partial update...")
        script = """
        local change_id = redis.call('zrangebyscore', KEYS[2], ARGV[1], '+inf', 'WITHSCORES')

        local partial_full_data = {}
        local i
        for i = 1, #change_id, 2 do
          table.insert(partial_full_data, change_id[i])
          table.insert(partial_full_data, redis.call('hget', KEYS[1], change_id[i]))
        end

        local result = {}
        table.insert(result, partial_full_data)
        table.insert(result, change_id)
        return result
        """
        change_id_ro = (
            self.change_id_ro + 1
            if isinstance(self.change_id_ro, int)
            else self.change_id_ro
        )
        partial_full_data, change_id = await self.eval_production_redis(
            script, keys=[self.full_data_key, self.change_id_key], args=[change_id_ro]
        )

        self.convert_change_id_list(change_id)

        # split partial_full_data into changed and deleted.
        changed_elements = []  # pairs of element_id, data
        deleted_elements = []  # element_ids
        for i in range(0, len(partial_full_data), 2):
            element_id = partial_full_data[i]
            element = partial_full_data[i + 1]
            if element:
                changed_elements.append(element_id)
                changed_elements.append(element)
            else:
                deleted_elements.append(element_id)

        if partial_full_data or change_id:
            transaction = self.ro_redis.multi_exec()
            if changed_elements:
                transaction.hmset(self.full_data_key, *changed_elements)
            if deleted_elements:
                transaction.hdel(self.full_data_key, *deleted_elements)
            if change_id:
                transaction.zadd(self.change_id_key, *change_id)
            await transaction.execute()
            logging.info("partial update done!")
        else:
            logging.info("nothing to update; partial update done!")

    async def eval_production_redis(self, script, keys=[], args=[]):
        hash = hashlib.sha1(script.encode()).hexdigest()
        try:
            return await self.production_redis.evalsha(hash, keys, args)
        except aioredis.errors.ReplyError as e:
            if str(e).startswith("NOSCRIPT"):
                return await self.production_redis.eval(script, keys, args)
            else:
                raise e

    def convert_change_id_list(self, change_id):
        # Invert the order of pairs from (value, score) to (score, value) and
        # convert the score to int.
        for i in range(0, len(change_id), 2):
            score = int(change_id[i + 1].decode())
            change_id[i + 1] = change_id[i]
            change_id[i] = score


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Redis-Synchronizer for ReadOnly-OpenSlides."
    )
    parser.add_argument(
        "-A", help="Production Redis address to read data from", default="127.0.0.1"
    )
    parser.add_argument("-P", help="Production Redis port", default="6379")
    parser.add_argument(
        "-a", help="Read-only Redis address to *write* data from", default="127.0.0.1"
    )
    parser.add_argument("-p", help="Read-only Redis port", default="6380")
    args = parser.parse_args()
    try:
        prod_redis = (args.A, int(args.P))
        ro_redis = (args.a, int(args.p))
    except ValueError:
        logging.critical("Both ports must be int")
        sys.exit(1)
    loop.run_until_complete(main(prod_redis, ro_redis))
