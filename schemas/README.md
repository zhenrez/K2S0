# Schema registry

| Schema | Canonical purpose |
| --- | --- |
| argocell-v1.schema.json | Recursive observed/desired/predicted Digital Twin IR |
| event-envelope-v1.schema.json | Ordered, idempotent, hash-chained event |
| event-envelope-v2.schema.json | Secured event envelope with authenticated producer role |
| projection-v1.schema.json | Purpose- and recipient-bound disclosed artifact |
| action-envelope-v1.schema.json | Separately authorized downstream action request |

JSON Schema defines storage and JSON interoperability. Protobuf defines the
high-throughput wire representation. Semantic changes require a new major
schema identifier; historical events are never rewritten in place.
