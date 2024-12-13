<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# Solr JMH Benchmark Module

![](https://user-images.githubusercontent.com/448788/140059718-de183e23-414e-4499-883a-34ec3cfbd2b6.png)

**_`profile, compare and introspect`_**

<samp>**A flexible, developer-friendly, microbenchmark framework**</samp>

![](https://img.shields.io/badge/developer-tool-blue)

## Table Of Content

- [](#)
  - [Table Of Content](#table-of-content)
  - [Overview](#overview)
  - [Getting Started](#getting-started)
    - [Running `jmh.sh` with no Arguments](#running-jmhsh-with-no-arguments)
    - [Pass a regex pattern or name after the command to select the benchmark(s) to run](#pass-a-regex-pattern-or-name-after-the-command-to-select-the-benchmarks-to-run)
    - [The argument `-l` will list all the available benchmarks](#the-argument--l-will-list-all-the-available-benchmarks)
    - [Check which benchmarks will run by entering a pattern after the -l argument](#check-which-benchmarks-will-run-by-entering-a-pattern-after-the--l-argument)
    - [Further Pattern Examples](#further-pattern-examples)
    - [`jmh.sh` accepts all the standard arguments that the standard JMH main-class handles](#jmhsh-accepts-all-the-standard-arguments-that-the-standard-jmh-main-class-handles)
    - [Overriding Benchmark Parameters](#overriding-benchmark-parameters)
    - [Format and Write Results to Files](#format-and-write-results-to-files)
  - [JMH Command-Line Arguments](#jmh-command-line-arguments)
    - [The JMH Command-Line Syntax](#the-jmh-command-line-syntax)
    - [The Full List of JMH Arguments](#the-full-list-of-jmh-arguments)
  - [Writing JMH benchmarks](#writing-jmh-benchmarks)
  - [Continued Documentation](#continued-documentation)

---

## Overview

JMH is a Java **microbenchmark** framework from some of the developers that work on
OpenJDK. Not surprisingly, OpenJDK is where you will find JMH's home today, alongside some
other useful little Java libraries such as JOL (Java Object Layout).

The significant value in JMH is that you get to stand on the shoulders of some brilliant
engineers that have done some tricky groundwork that many an ambitious Java benchmark writer
has merrily wandered past.

Rather than simply providing a boilerplate framework for driving iterations and measuring
elapsed times, which JMH does happily do, the focus is on the many forces that
deceive and disorient the earnest benchmark enthusiast.

From spinning your benchmark into all new generated source code
in an attempt to avoid falling victim to undesirable optimizations, to offering
**BlackHoles** and a solid collection of convention and cleverly thought out yet
simple boilerplate, the goal of JMH is to lift the developer off the
microbenchmark floor and at least to their knees.

JMH reaches out a hand to both the best and most regular among us in a solid, cautious
effort to promote the willing into the real, often-obscured world of the microbenchmark.

## Code Organization Breakdown

![](https://img.shields.io/badge/data-...move-blue)

- **JMH:** microbenchmark classes and some common base code to support them.

- **Random Data:** a framework for easily generating specific and repeatable random data.

## Getting Started

Running **JMH** is handled via the `jmh.sh` shell script. This script uses Gradle to
extract the correct classpath and configures a handful of helpful Java
command prompt arguments and system properties. For the most part, `jmh.sh` script
will pass any arguments it receives directly to JMH. You run the script
from the root benchmark module directory (i.e. `solr/benchmark`).

### Running `jmh.sh` with no Arguments

>
> ```zsh
> # run all benchmarks found in subdirectories
> ./jmh.sh
> ```

### Pass a regex pattern or name after the command to select the benchmark(s) to run

>
> ```zsh
> ./jmh.sh BenchmarkClass 
> ```

### The argument `-l` will list all the available benchmarks

>
> ```zsh
> ./jmh.sh -l
> ```

### Check which benchmarks will run by entering a pattern after the -l argument

Use the full benchmark class name, the simple class name, the benchmark
method name, or a substring.

>
> ```zsh
> ./jmh.sh -l Ben
> ```

### Further Pattern Examples

>
> ```shell
>./jmh.sh -l org.apache.solr.benchmark.search.BenchmarkClass
>./jmh.sh -l BenchmarkClass
>./jmh.sh -l BenchmarkClass.benchmethod
>./jmh.sh -l Bench
>./jmh.sh -l benchme

### The JMH Script Accepts _ALL_ of the Standard JMH Arguments

Here we tell JMH to run the trial iterations twice, forking a new JVM for each
trial. We also explicitly set the number of warmup iterations and the
measured iterations to 2.

>
> ```zsh
> ./jmh.sh -f 2 -wi 2 -i 2 BenchmarkClass
> ```

### Overriding Benchmark Parameters

> ![](https://img.shields.io/badge/overridable-params-blue)
>
> ```java
> @Param("1000")
> private int numDocs;
> ```

The state objects that can be specified in benchmark classes will often have a
number of input parameters that benchmark method calls will access. The notation
above will default numDocs to 1000 and also allow you to override that value
using the `-p` argument. A benchmark might also use a @Param annotation such as:

> ![](https://img.shields.io/badge/sequenced-params-blue)
>
> ```java
> @Param("1000","5000","1000")
> private int numDocs;
> ```

By default, that would cause the benchmark
to be run enough times to use each of the specified values. If multiple input
parameters are specified this way, the number of runs needed will quickly
expand. You can pass multiple `-p`
arguments and each will completely replace the behavior of any default
annotation values.

>
> ```zsh
> # use 2000 docs instead of 1000
> ./jmh.sh BenchmarkClass -p numDocs=2000
>
>
> # use 5 docs, then 50, then 500
> ./jmh.sh BenchmarkClass -p numDocs=5,50,500
>
>
> # run the benchmark enough times to satisfy every combination of two
> # multi-valued input parameters
> ./jmh.sh BenchmarkClass -p numDocs=10,20,30 -p docSize 250,500
> ```

### Format and Write Results to Files

Rather than just dumping benchmark results to the console, you can specify the
`-rf` argument to control the output format; for example, you can choose CSV or
JSON. The `-rff` argument will dictate the filename and output location.

>
> ```zsh
> # format output to JSON and write the file to the `work` directory relative to
> # the JMH working directory.
> ./jmh.sh BenchmarkClass -rf json -rff work/jmh-results.json
> ```
>
> 💡 **If you pass only the `-rf` argument, JMH will write out a file to the
> current working directory with the appropriate extension, e.g.,** `jmh-results.csv`.

## JMH Command-Line Arguments

### The JMH Command-Line Syntax

> ![](https://img.shields.io/badge/Help-output-blue)
>
> ```zsh
> Usage: ./jmh.sh [regexp*] [options]
> [opt] means optional argument.
> <opt> means required argument.
> "+" means comma-separated list of values.
> "time" arguments accept time suffixes, like "100ms".
>
> Command-line options usually take precedence over annotations.
> ```

### The Full List of JMH Arguments

```zsh

 Usage: ./jmh.sh [regexp*] [options]
 [opt] means optional argument.
 <opt> means required argument.
 "+" means a comma-separated list of values.
 "time" arguments accept time suffixes, like "100ms".

Command-line options usually take precedence over annotations.

  [arguments]                 Benchmarks to run (regexp+). (default: .*) 

  -bm <mode>                  Benchmark mode. Available modes are: 
                              [Throughput/thrpt,  AverageTime/avgt, 
                              SampleTime/sample, SingleShotTime/ss, 
                              All/all]. (default: Throughput) 

  -bs <int>                   Batch size: number of benchmark method calls per 
                              operation. Some benchmark modes may ignore this 
                              setting; please check this separately. 
                              (default: 1) 

  -e <regexp+>                Benchmarks to exclude from the run. 

  -f <int>                    How many times to fork a single benchmark. Use 0
                              to  disable forking altogether. Warning:
                              disabling  forking may have a detrimental impact on
                              benchmark and infrastructure reliability. You might
                              want to use a different warmup mode instead. (default: 1) 

  -foe <bool>                 Should JMH fail immediately if any benchmark has
                              experienced an unrecoverable error? Failing fast
                              helps to make quick sanity tests for benchmark
                              suites and allows automated runs to do error
                              checking.
                              codes. (default: false) 

  -gc <bool>                  Should JMH force GC between iterations? Forcing 
                              GC may help lower the noise in GC-heavy benchmarks
                              at the expense of jeopardizing GC ergonomics
                              decisions. 
                              Use with care. (default: false) 

  -h                          Displays this help output and exits. 

  -i <int>                    Number of measurement iterations to do.
                              Measurement 
                              iterations are counted towards the benchmark
                              score. 
                              (default: 1 for SingleShotTime, and 5 for all
                              other modes) 

  -jvm <string>               Use given JVM for runs. This option only affects
                              forked  runs. 

  -jvmArgs <string>           Use given JVM arguments. Most options are 
                              inherited from the host VM options, but in some
                              cases, you want to pass the options only to a forked
                              VM. Either single space-separated option line or 
                              multiple options are accepted. This option only
                              affects forked runs. 

  -jvmArgsAppend <string>     Same as jvmArgs, but append these options after
                              the  already given JVM args. 

  -jvmArgsPrepend <string>    Same as jvmArgs, but prepend these options before 
                              the already given JVM arg. 

  -l                          List the benchmarks that match a filter and exit. 

  -lp                         List the benchmarks that match a filter, along
  with 
                              parameters, and exit. 

  -lprof                      List profilers and exit. 

  -lrf                        List machine-readable result formats and exit. 

  -o <filename>               Redirect human-readable output to a given file. 

  -opi <int>                  Override operations per invocation, see 
                              @OperationsPerInvocation  Javadoc for details.
                              (default: 1) 

  -p <param={v,}*>            Benchmark parameters. This option is expected to
                              be used once per parameter. The parameter name and
                              parameter values should be separated with an
                              equal sign. Parameter values should be separated
                              with commas. 

  -prof <profiler>            Use profilers to collect additional benchmark
  data. 
                              Some profilers are not available on all JVMs or
                              all OSes.  '-lprof' will list the available 
                              profilers that are available and that can run
                              with the current OS configuration and installed dependencies.

  -r <time>                   Minimum time to spend at each measurement
                              iteration. Benchmarks may generally run longer
                              than the iteration duration. (default: 10 s) 

  -rf <type>                  Format type for machine-readable results. These 
                              results are written to a separate file
                              (see -rff).  See the list of available result
                              formats with -lrf. 
                              (default: CSV) 

  -rff <filename>             Write machine-readable results to a given file. 
                              The -rf option controls the file format. Please
                              see  the list of result formats available. 
                              (default: jmh-result.<result-format>) 

  -si <bool>                  Should JMH synchronize iterations? Doing so would
                              significantly lower the noise in multithreaded
                              tests by ensuring that the measured part happens
                              when all workers are running.
                              (default: true) 

  -t <int>                    Number of worker threads to run with. 'max' means
                              the maximum number of hardware threads available
                              the machine, figured out by JMH itself. 
                              (default:  1) 

  -tg <int+>                  Override thread group distribution for asymmetric 
                              benchmarks. This option expects a comma-separated 
                              list of thread counts within the group. See 
                              @Group/@GroupThreads 
                              Javadoc for more information. 

  -to <time>                  Timeout for benchmark iteration. After reaching
                              this timeout, JMH will try to interrupt the running
                              tasks. Non-cooperating benchmarks may ignore this =
                              timeout. (default: 10 min) 

  -tu <TU>                    Override time unit in benchmark results. Available
                              time units are: [m, s, ms, us, ns].
                              (default: SECONDS) 

  -v <mode>                   Verbosity mode. Available modes are: [SILENT,
                              NORMAL, EXTRA]. (default: NORMAL) 

  -w <time>                   Minimum time to spend at each warmup iteration.
                              Benchmarks 
                              may generally run longer than iteration duration. 
                              (default: 10 s) 

  -wbs <int>                  Warmup batch size: number of benchmark method
                              calls  per operation. Some benchmark modes may
                              ignore this  setting. (default: 1) 

  -wf <int>                   How many warmup forks to make for a single
                              benchmark.   All benchmark iterations within the
                              warmup fork do not count towards the benchmark score.
                              Use 0 to disable warmup forks. (default: 0) 

  -wi <int>                   Number of warmup iterations to do. Warmup
                              iterations do not count towards the benchmark
                              score. 
                              (default:  0 for SingleShotTime, and 5 for all other
                              modes) 

  -wm <mode>                  Warmup mode for warming up selected benchmarks. 
                              Warmup modes are INDI = Warmup each benchmark
                              individually, 
                              then measure it. BULK = Warm up all benchmarks
                              first, then do all the measurements. BULK_INDI =
                              warmup all benchmarks first, then re-warm up each
                              benchmark individually, then measure it. 
                              (default: INDI) 

  -wmb <regexp+>              Warmup benchmarks to include in the run, in
                              addition to already selected by the primary filters.
                              The harness will not measure these benchmarks but only
                              use them for the warmup. 
```

</details>

---

## Writing JMH benchmarks

For additional insight around writing correct JMH tests, the best place to start is
the [sample code](https://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/)
provided by the JMH project.

JMH is highly configurable, and users are encouraged to look through the samples
for exposure around what options are available. A good tutorial for learning JMH basics is
found [here](http://tutorials.jenkov.com/java-performance/jmh.html#return-value-from-benchmark-method)

## Additional Documentation

### 📚 Profilers

JMH is compatible with a number of profilers that can be used both to (1)
validate that benchmarks are measuring the things they intend to, and (2) to
identify performance bottlenecks and hotspots.

- 📒 [docs/jmh-profilers.md](docs/jmh-profilers.md)
- 📒 [docs/jmh-profilers-setup.md](docs/jmh-profilers-setup.md)
