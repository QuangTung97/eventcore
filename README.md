# Library for Transactional Outbox Pattern with Hybrid Approach
*A replacement for Debezium & Kafka without tuning the polling period for low latency*

## With the following features:
* Guarantee At-Least-Once Delivery.
* Total ordering of events.
* Low latency.
* Fault tolerance (but required must run **ONLY** one instance, so not *Highly Available*).
* Support multiple publishers, both synchronous and asynchronous.

## How it works
It's a library for building a Message Relay in the
**Transactional Outbox Pattern**
https://microservices.io/patterns/data/transactional-outbox.html

But, instead of polling the database in some fixed interval,
it's required the **Service** to send signals to the Message Relay for
active polling. The Message Relay will start polling right after receiving the signal.

The Message Relay will assign the **Sequence** values of
polled messages and save back to the Database to guarantee **Total Ordering of Events**.

It's will automatically retry indefinitely if something went wrong.