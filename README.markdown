# beanstalk

A native clojure [beanstalkd](http://kr.github.com/beanstalkd/) client library. 
Some inspiration and ideas were taken from [cl-beanstalk](https://github.com/antifuchs/cl-beanstalk/).

WARNING: Interface subject to change. This interface is not considered
idiomatic clojure. A more "clojure" interface would probably look something
like the following:

    (beanstalk/with-server config-map
        (use "my-tube")
        (put 0 0 0 5 "hello"))

    (beanstalk/with-server config-map
        (watch "my-tube")
        (let [job (reserve)]
            (do-something-with-job)))

See also [À la carte configuration in Clojure APIs](http://cemerick.com/2011/10/17/a-la-carte-configuration-in-clojure-apis/).

## Usage

The beanstalk client uses deftype and a protocol declaration to create a simple 
(socket, reader, writer) tuple. The protocol defines methods that are direct 
mappings to the [beanstalk protocol commands](https://github.com/kr/beanstalkd/blob/v1.3/doc/protocol.txt). 
For example: 

    (:use com.github.drsnyder.beanstalk)

    ; producer
    user=> (def b (new-beanstalk))
    user=> (.use b "my-tube")
    user=> (.put b 0 0 0 5 "hello")
    ...
    ; consumer
    user=> (def b (new-beanstalk))
    user=> (.watch b "my-tube")
    user=> (def job (.reserve b)) ; id is (:id job), payload is (:payload job)

This library is also available on [clojars](https://clojars.org/com.github.drsnyder/beanstalk). 
To use, add [com.github.drsnyder/beanstalk "1.0.0-SNAPSHOT"] to your dependencies.

## Examples

Two examples are provided:

    ; start consumer
    lein run -m com.github.drsnyder.beanstalk.examples.consumer 

    ; send some data
    lein run -m com.github.drsnyder.beanstalk.examples.producer -m "hello" -n 5 
    ; send shutdown
    lein run -m com.github.drsnyder.beanstalk.examples.producer -m "exit"

## License

Copyright (c) 2010 Damon Snyder 

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
