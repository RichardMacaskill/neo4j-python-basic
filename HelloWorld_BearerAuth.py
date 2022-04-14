import logging
import sys

import neo4j
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable


class App:

    def __init__(self, uri, access_token):
        self.driver = GraphDatabase.driver(uri, auth=access_token)

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    @staticmethod
    def enable_log(level, output_stream):
        handler = logging.StreamHandler(output_stream)
        handler.setLevel(level)
        logging.getLogger("neo4j").addHandler(handler)
        logging.getLogger("neo4j").setLevel(level)

    def create_friendship(self, person1_name, person2_name, knows_from):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_and_return_friendship, person1_name, person2_name, knows_from)
            for row in result:
                print("Created friendship between: {p1}, {p2} from {knows_from}"
                      .format(
                          p1=row['p1'],
                          p2=row['p2'],
                          knows_from=row["knows_from"]))

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name, knows_from):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = (
            "CREATE (p1:Person { name: $person1_name }) "
            "CREATE (p2:Person { name: $person2_name }) "
            "CREATE (p1)-[k:KNOWS { from: $knows_from }]->(p2) "
            "RETURN p1, p2, k"
        )
        result = tx.run(query, person1_name=person1_name,
                        person2_name=person2_name, knows_from=knows_from)
        try:
            return [{
                        "p1": row["p1"]["name"],
                        "p2": row["p2"]["name"],
                        "knows_from": row["k"]["from"]
                    }
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_person(self, person_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_person, person_name)
            for row in result:
                print("Found person: {row}".format(row=row))

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = (
            "MATCH (p:Person) "
            "WHERE p.name = $person_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, person_name=person_name)
        return [row["name"] for row in result]


if __name__ == "__main__":
    bolt_url = "bolt+s://cas-testlab-neo4j.co.uk:7687"
    my_access_token = neo4j.bearer_auth(
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImpTMVhvMU9XRGpfNTJ2YndHTmd2UU8yVnpNYyJ9.eyJhdWQiOiIyN2EyMTMxYS01MmRjLTRhNTctYmI0My1kN2Y3NTNjNDRjN2EiLCJpc3MiOiJodHRwczovL2xvZ2luLm1pY3Jvc29mdG9ubGluZS5jb20vNTU1ZWU3ZGQtNTUyNi00YjNkLWEzNWYtYjg1MjYzYjExNGU3L3YyLjAiLCJpYXQiOjE2NDg1MzkyNzMsIm5iZiI6MTY0ODUzOTI3MywiZXhwIjoxNjQ4NTQzMTczLCJhaW8iOiJBV1FBbS84VEFBQUE0TnRWTWZPVkI1dEdpYTVaQ0xpY0tPWGVWWkdiVS9RU0dEbS9nOUxnQjZXTXJzRG0zcmZwc0RrdjRNeU9iRjNIUGZGWDNiVWkrcDNZUlE1K2JnakpEN2xweTIwMmJnLzlrVVR0eTcyRWxRWnNRK2kya21qTlQ2bzBnRnFiRHJTNiIsImVtYWlsIjoicmljaGFyZC5tYWNhc2tpbGxAb3V0bG9vay5jb20iLCJpZHAiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC85MTg4MDQwZC02YzY3LTRjNWItYjExMi0zNmEzMDRiNjZkYWQvIiwibmFtZSI6IlJpY2ggTWFjYXNraWxsIiwib2lkIjoiYTJmZWM0YjUtYWM5NC00NmFlLTgwNzYtZmRmMTQzM2RmY2NlIiwicHJlZmVycmVkX3VzZXJuYW1lIjoicmljaGFyZC5tYWNhc2tpbGxAb3V0bG9vay5jb20iLCJyaCI6IjAuQVFzQTNlZGVWU1pWUFV1alg3aFNZN0VVNXhvVG9pZmNVbGRLdTBQWDkxUEVUSG9MQUgwLiIsInJvbGVzIjpbImFkbWluIl0sInN1YiI6InljZjhuNGJrRjd6eWhmNHlYdHJuNlFrVjBLajNWZWJXUjJURzA2X3M2SDQiLCJ0aWQiOiI1NTVlZTdkZC01NTI2LTRiM2QtYTM1Zi1iODUyNjNiMTE0ZTciLCJ1dGkiOiJXTldxMERXX19FRzBaLUtBQy04eEFBIiwidmVyIjoiMi4wIn0.RZDCpTMijQwQT0auuT3rkgcVAplHJE-PhTBcWwbQnxfXMQ86qJfixzkx2GGahvaqisjoaXNUtNzAFrKz_436DGTPZOHnAbk4PVejGiYrX2UKqB1K7im5MqnBq5W1KLIMY-V75swc5pnHXcn_WeQcJx2THcWUTP2g0YHPeuGZMxNrpvU6CcKX9qaRAMQBeYFL7ug92c3UpRvQ9ADL-HiNePDbg5Ls-xm75RYuRP8ebzoZPy9e19OM72fSI5cTT-qH9c9-11zGP_uzOVU88H9u8ZCYTmeUALccbfb_Y2vyVgnYD126_UVoCw0INt7ZznfHkLxHKeXL8qorP9l41dC71Q")
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_url, my_access_token)
    app.create_friendship("Alice", "David", "School")
    app.find_person("Alice")
    app.close()