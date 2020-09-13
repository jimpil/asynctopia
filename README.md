# asynctopia

## What
A Clojure library designed to assist with asynchronous processing (in the context of `core.async`).

## Why
`core.async` is a sharp tool. One can do wonderful or terrible things with a sharp tool.
The primary goal here is to provide solid high-level constructs in order to help people avoid common traps and pitfalls. 
As a secondary goal I'm trying to provide some trivial utilities that have been requested through the official `core.async` JIRA.
Finally, I realised that channel buffers do NOT need to be type-hinted as `LinkedList` - a mere `Deque` suffices. 
This alone opens up the door for more flexible constructor functions that would allow any `Deque` impl - in fact `ArrayDeque` 
sounds like a great candidate for a default, but `LinkedList` can still be supported. Therefore, I went ahead and implemented 
two sets (regular VS thread-safe) of the three buffers (fixed, dropping, sliding), along with a convenient constructor function 
that supports everything (see `buffers/buf` leveraged by `channels/chan`).

## Where
FIXME

## How
### asynctopia.null
In typical Clojure code `nil` is not something to be terrified of. Any function invocation can potentially return nil, 
and in many contexts it will have a meaning (e.g. there are no more elements in this collection). There is even a higher 
level construct (`fnil`) to assist with consuming a potentially dangerous `nil`.

Contrast that with typical `core.async` code, where `nil` is simply not allowed. What do you do with all the 
functions (you already have) that may produce `nil`, or the ones that are able to consume `nil`? 
This namespace tries to address this by establishing a symmetric convention (on the way in, and on the way out).
 
Writers (way in) will want to use `replacing` right before putting, whereas readers (way out) will want to 
use `restoring` right after taking. Following this pattern/convention provides nil-safety to producers 
(`nil` may be a perfectly valid value, but cannot be conveyed via a channel), and transparency to consumers 
(they will receive what was actually produced). Without this dance, you have to carefully vet every single producing-fn 
that interacts with `core.async`, and that is not always easy.
  
See `asynctopia.ops/>!?` for a higher-level construct that makes use of this, and is a good candidate for writers 
(as opposed to the lower-level `converting` macro). 

### asynctopia.ops
#### pipe-with \[f from & {:keys \[error! buffer\]}\]
Pipes the `from` channel into a newly created output-channel 
returning the latter. Errors thrown by `f` are handled with `error!` (defaults to simply printing the error message). 

Honors the `asynctopia.null` convention. 

#### sink-with \[f ch error!\]
Generic sinking `go-loop`. Fully consumes the provided channel 
passing the taken values through `f` (presumably side-effecting, ideally non-blocking).
Errors thrown by `f` are handled with `error!` (defaults to simply printing the error message).
Honors the `asynctopia.null` convention. 

#### mix-with \[f out-chan in-chans error!\]
Creates a `mix` against `out-chan`, adds all `in-chans` to it,
and starts sinking `out-chan` with `f`. Returns the `mix` object.
Honors the `asynctopia.null` convention. 

You will need to close `out-chan` in order to stop the mixing/sinking loops (i.e. closing `in-chans` or unmixing  won't suffice). 

#### drain \[ch\]
Drains the provided channel, disregarding its contents.

#### <!?deliver \[ch p\]
Takes from channel `ch` and delivers the value to promise `p`.
Honors the `asynctopia.null` convention. 

#### >!? \[ch x\]
Nil-safe variant of `>!` (i.e. If `x` is `nil`, will put `:asynctopia.null/nil`). 
Encapsulates half of the `asynctopia.null` convention (writing). 
The other half (reading) must be done manually.

#### merge-reduce \[f init chan & chans\]
Merges all channels and reduces them with `f`. 
Honors the `asynctopia.null` convention. 

### asynctopia.core
#### consuming-with \[consume! from & {:keys \[error? error! to-error buffer\]}\]
Sets up two `go` loops for consuming values VS errors (identified by the `error?` predicate) 
from the `from` channel. Values are consumed via `consume!`, whereas errors via `error!`.
The ideal consuming fns would submit to some thread-pool, send-off to some agent, 
or something along these lines (i.e. non-blocking). Both `go` loops gracefully terminate when `from` is closed.
Honors the `asynctopia.null` convention.

The three optional error fns are semantically related. Observe the defaults as a guide: 
 
 - `to-error` default uses `identity` (returns the Throwable itself) 
 - `error?` default recognises the Throwable
 - `error!` default prints (via `println`) the message of the Throwable

If for example your `to-error` fn constructed an error-map (from the Throwable), then your `error?` 
would typically check that the map contains some error key, and your `error!` would do something 
with the value of that key. 

#### pkeep \[f coll & {:keys \[error! in-flight buffer blocking-input?\]\]}
Parallel keep (bounded by `in-flight`) of `f` across `coll` (via `pipeline-blocking`).
Errors thrown by `f` are handled with `error!`. `blocking-input?` controls how to turn `coll`
into an input channel (`to-chan!` VS `to-chan!!`), whereas `buffer` controls how the output channel
will be buffered (defaults to `(count coll)`). Returns a channel containing the single 
(collection) result (i.e. take one item). Does NOT honor the `asynctopia.null` convention 
(and that's why it's not called `pmap`).  

#### with-timeout \[ms timeout-val & body\]
Returns a channel which will (eventually) receive either the result of `(do body)`, 
or `timeout-val` (whichever comes first).

### asynctopia.pubsub 
#### pub-sub! \[in topic-fn topics & {:keys \[error? error! to-error topic->buffer topic->nconsumers\]\]}
Configuration-driven (`topics`) pub-sub infrastructure.
Creates a `pub` against channel `in` with `topic-fn` and`topic->buffer` 
(maps topic-keys to custom buffers - see `pub`).
Then sets up subscriptions against the publication for all topics.
`topics` must be either a map from topic to its process-fn (1-arg), 
or a vector of two elements - the list of topics followed by a common 
process-fn (2-arg, the topic-key first). Finally, and since we know 
the processing-fn per topic, sets-up consuming loops (again buffered per `topic->buffer`) 
for all subscriptions. The number of consuming loops per subscription is controlled by 
`topic->nconsumers` (a map from topic-key => positive integer, defaults to 1).
Returns a vector of 2 elements - the publication, and the subscription channels.
This can form the basis of a simple (in-process) event-bus (events arrive,
and distributed to their respective topic processors).

### asynctopia.buffers
#### buf \[buf-or-n thread-safe?\]
The only buffer constructor-fn you will ever need. Apart from a number,
it also supports a vector of 2-4 elements `[semantics n dq react!]`. 

- semantics: one of `:fixed`/`:dropping`/`:sliding`
- n: a positive integer - defaults to 1024
- dq: any instance of `Deque` (NOT a `ConcurrentLinkedDeque` - see `thread-safe?` for that) - defaults to `ArrayDeque`
- react!: a 1-arg fn to be called when an item is dropped (`:dropping` semantics), or slided (`:sliding` semantics)

When `thread-safe?` is true (NOT the default), the returned buffer will be backed by a `ConcurrentLinkedDeque`, 
thus supporting safe snapshotting (see `proto/snapshot`).

### asynctopia.channels
#### chan \[buf-or-n xform ex-handler thread-safe-buffer?\]
Drop-in replacement for `clojure.async.core/chan`, supporting any `Deque` buffer (not just `LinkedList`).
See `buffers/buf` for an explanation on what `buf-or-n` can be, and what `thread-safe-buffer?` means.

#### line-chan \[src buf-or-n xform ex-handler\]
Returns a channel that will receive all the lines in <src> (via `line-seq`) transformed per <xform>.

#### counting-chan \[ch\]
Returns a channel that will receive the total number of elements taken from `ch`.

#### throttled-chan \[ch rate unit bucket-size\]
This was taken/adapted from [throttler](https://github.com/brunoV/throttler).

Takes a write channel, a goal rate and a unit and returns a read channel. 
Messages written to the input channel can be read from the throttled output channel at a maximum `rate`.
The `bucket-size` controls burstiness  (defaults to 1). 


## License

Copyright © 2020 Dimitrios Piliouras

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
