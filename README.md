# Data Platform Common

A set of common libraries and utils for the whole data platform.

## Build

To build this repository you need to create a new (update existing) file:

```bash
$HOME/.gradle/gradle.properties
```

With following content:

```bash
vstsMavenAccessToken={YOUR_MAVEN_REPOSITORY_TOKEN}
artifactVersion=1.0.0
artifactDir=
```

## Spark Applications

There is a limitation in Scala: "main" method cannot be found in companion object. So names should be different.

```scala
class SegmentationApplication extends SparkApplication[SegmentConfig] { ... }

object SegmentationApp extends App {
  new SegmentationApplication().main(args)
}
```