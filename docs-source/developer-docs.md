---
id: developerdocs
title: Developer documentation
sidebar_label: Developer documentation
---
## Running the tests
1. Spin up an ElasticSearch instance locally, for example with docker you could do:
```bash
docker run -p 9200:9200 -p 9300:9300 -e discovery.type=single-node elasticsearch:7.1.0
```

2. Run the tests as usual with sbt
```bash
sbt test
```

## Published with SBT Sonatype
https://github.com/xerial/sbt-sonatype

To publish a new version do the following in an sbt shell:
```
release
```

## Documentation creation and publishing
Sources for documentation are in the `docs-sources` folder.

To update the documentation from the docs-sources folder run:
```
sbt docs/mdoc
```

To update API docs run:
```
sbt docs/unidoc
```

To run the documentation site locally run:
```
cd website && yarn start
```

This will check that the Scala code compiles and make any required variable substitutions.

Changes are automatically published to Github Pages when code is merged to master. However if you wish to publish to
Github Pages locally then run:
```
cd website && GITHUB_USER=xxxx CURRENT_BRANCH=xxxx USE_SSH=true yarn run publish-gh-pages
```