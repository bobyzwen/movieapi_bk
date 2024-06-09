docker run -i \
-v $(pwd):/deck \
kong/deck --kong-addr http://host.docker.internal:8001 --headers kong-admin-token:KONG_ADMIN_TOKEN -o /deck/kong.yaml dump