curl --request GET \
  --url https://$ASTRA_DB_ID-$ASTRA_DB_REGION.apps.astra.datastax.com/api/rest/v2/namespaces/$ASTRA_DB_KEYSPACE/collections/demo_book\
  -H "X-Cassandra-Token: $ASTRA_DB_APPLICATION_TOKEN" \
  -H 'Content-Type: application/json'