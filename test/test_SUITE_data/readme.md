Generate token key and token with the following commands (run inside Pulsar container):

```sh
bin/pulsar tokens create-key-pair --output-private-key my-private.key --output-public-key my-public.key
bin/pulsar tokens create --secret-key file:///pulsar/my-private.key --subject admin
```
