Set Java versions to 17

In Run Configurations got to `Modify options` -> `Add VM options` and set value as `--add-exports java.base/sun.nio.ch=ALL-UNNAMED`

Also got to `Modify options` -> `Add dependencies with "provided" scope to classpath` mark checked

![IntelliJ - Run Configurations](https://github.com/officiallysingh/spark-try-me/blob/main/IntelliJ-Run%20Configuration.png)