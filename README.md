# go-ds-pebble

[![codecov](https://codecov.io/gh/RTradeLtd/go-ds-pebble/branch/master/graph/badge.svg)](https://codecov.io/gh/RTradeLtd/go-ds-pebble) [![Build Status](https://travis-ci.com/RTradeLtd/go-ds-pebble.svg?branch=master)](https://travis-ci.com/RTradeLtd/go-ds-pebble)

Datastore implementation using [github.com/cockroachdb/pebble](https://github.com/cockroachdb/pebble).

Please be cautious about using this in your code. Although pebble works, there is an explicit warning about using pebble. See the comment from pebble maintainer [`petermattis`](https://github.com/petermattis/pebble/issues/168#issuecomment-507042998) for more information.
