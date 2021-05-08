from astrapy.rest import create_client, http_methods
import uuid, os


def main():
    """The main routine."""
    astra_http_client = getAstraHTTPClient()
    createJSONonAstra(astra_http_client)


def getAstraHTTPClient():
    """Get Astra connection information from environment variables"""

    ASTRA_DB_ID = os.environ.get('ASTRA_DB_ID')
    ASTRA_DB_REGION = os.environ.get('ASTRA_DB_REGION')
    ASTRA_DB_APPLICATION_TOKEN = os.environ.get('ASTRA_DB_APPLICATION_TOKEN')
    
    # setup an Astra Client
    return create_client(astra_database_id=ASTRA_DB_ID,
                         astra_database_region=ASTRA_DB_REGION,
                         astra_application_token=ASTRA_DB_APPLICATION_TOKEN)


def createJSONonAstra(astra_http_client):
    """Create a document on Astra using the Document API"""

    doc_uuid = uuid.uuid4()
    ASTRA_DB_KEYSPACE = os.environ.get('ASTRA_DB_KEYSPACE')
    ASTRA_DB_COLLECTION = os.environ.get('ASTRA_DB_COLLECTION')

    astra_http_client.request(
        method=http_methods.PUT,
        path=f"/api/rest/v2/namespaces/{ASTRA_DB_KEYSPACE}/collections/{ASTRA_DB_COLLECTION}/{doc_uuid}",
        json_data={
            "book": "The Hunger Games",
            "author": "Suzanne Collins",
            "genre": ["fiction"],
        })


if __name__ == "__main__":
    main()