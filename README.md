# catalyst-api

An HTTP API to allow services (e.g Livepeer Studio) to interact with Catalyst.

## Prerequisites

The default Makefile goal and the CI process for this repo uses `golangci-lint` for linting. To install, follow the instructions at https://golangci-lint.run/usage/install/#local-installation

## Development

To test the [Catalyst](http://github.com/livepeer/catalyst) integration, follow the instructions in that repo to run a local Catalyst instance and then:

```
make run
```

This will give you another local Catalyst API instance (in addition to the one running as part of the full Catalyst setup).

By default, this runs on a different port (`4949`) to the Catalyst one (`7979`) and so to test it, run:

```
curl -I 'http://localhost:4949/ok'
```

If you see a response body of `OK` then things are working and you can begin using this instance to test the API's integration with Mist and the other parts of the Catalyst system.
