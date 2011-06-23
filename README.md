# BsonNetwork

### Networking as simple as a dictionary!

## About

BsonNetwork is a networking library built on top of gevent (older versions used
twisted). It tries to facilitate writing services with simple protocols using
[http://bsonspec.org/](BSON) as the encoding format. It attempts to provide a
protocol abstraction in a middle-ground between twisted and gevent, leveraging
the ease of writing response based protocols a-la-twisted, and gevent's
non-blocking synchronous API.

The sister project, bsonnetwork-objc facilitates interfacing with this library,
though that project is not as in depth as this one.

## Install

    sudo python setup.py install

## License

BsonNetwork is under the MIT License.
