from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
import logging
from neo4j.exceptions import ServiceUnavailable

load_dotenv()
URI = os.environ.get('NEO4J_URI')
AUTH = (os.environ.get('NEO4J_USERNAME'), os.environ.get('NEO4J_PASSWORD'))

class App:

    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)
        self.driver.verify_connectivity()

    def close(self):
        self.driver.close()

    def append_account(self, address):
        with self.driver.session(database="neo4j") as session:
            try:
                result = session.execute_write(self._append_and_return_account, address)
                print(result)
                print("Created Account {}".format(result))
            except Exception as e:
                print(e)

    @staticmethod
    def _append_and_return_account(tx, address):
        result = tx.run("""
                        MERGE (addr: account {address: $address})
                        ON CREATE SET addr.isContract = False, addr.isNormal = False
                        RETURN addr.address AS id
                        """, address=address)
        return result.single()["id"]
        
    def append_tx(self, address1, address2, blocknum, timestamp, value, hash_num):
        with self.driver.session(database="neo4j") as session:
            try:
                result = session.execute_write(self._append_and_return_tx, address1, address2, blocknum, timestamp, value, hash_num)

                for row in result:
                    print("Create tx between: {addr1}, {addr2}".format(addr1=row['addr1'], addr2=row['addr2']))
            except Exception as e:
                print(e)
    
    @staticmethod
    def _append_and_return_tx(tx, address1, address2, blocknum, timestamp, value, hash_num):
        result = tx.run("""
                        MERGE (addr1: account {address: $address1})
                        ON CREATE SET addr1.isContract = False, addr1.isNormal = False
                        MERGE (addr2: account {address: $address2})
                        ON CREATE SET addr2.isContract = False, addr2.isNormal = False
                        MERGE (addr1)-[:TX {hash: $hash, blocknum: $blocknum, timestamp:$timestamp, value: $value}]->(addr2)
                        RETURN addr1, addr2
                        """, address1=address1, address2=address2, hash=hash_num, blocknum=blocknum, timestamp=timestamp, value=value)
        try:
            return [{"addr1": row["addr1"]["address"], "addr2": row["addr2"]["Address"]} for row in result]
        except ServiceUnavailable as excetion:
            logging.error("query raised an error: \n {exception}".format(excetion))
            raise


if __name__ == "__main__":
    app = App(URI, AUTH)
    app.append_account("0x963737C550E70FFe4D59464542a28604eDb2eF9a")
    app.close()