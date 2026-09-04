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

# JMH Profiler Setup (Async-Profiler and Perfasm)

JMH ships with a number of built-in profiler options that have grown in number over time. The profiler system is also pluggable,
allowing for "after-market" profiler implementations to be added on the fly.

Many of these profilers, most often the ones that stay in the realm of Java, will work across platforms and architectures and do
so right out of the box. Others may be targeted at a specific OS, though there is a good chance a similar profiler for other OS's
may exist where possible. A couple of very valuable profilers also require additional setup and environment to either work fully
or at all.

[TODO: link to page that only lists commands with simple section]

- [JMH Profiler Setup (Async-Profiler and Perfasm)](#jmh-profiler-setup-async-profiler-and-perfasm)
    - [Async-Profiler](#async-profiler)
      - [Install async-profiler](#install-async-profiler)
      - [Install Java Debug Symbols](#install-java-debug-symbols)
        - [Ubuntu](#ubuntu)
        - [Arch](#arch)
  - [Perfasm](#perfasm)
    - [Arch](#arch-1)
    - [Ubuntu](#ubuntu-1)

<br/>
This guide will cover setting up both the async-profiler and the Perfasm profiler. Currently, we roughly cover two Linux family trees,
but much of the information can be extrapolated or help point in the right direction for other systems.

<br/> <br/>

|<b>Path 1: Arch, Manjaro, etc</b>|<b>Path 2: Debian, Ubuntu, etc</b>|
| :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
| <image src="https://user-images.githubusercontent.com/448788/137563725-0195a732-da40-4c8b-a5e8-fd904a43bb79.png"/><image src="https://user-images.githubusercontent.com/448788/137563722-665de88f-46a4-4939-88b0-3f96e56989ea.png"/> | <image src="https://user-images.githubusercontent.com/448788/137563909-6c2d2729-2747-47a0-b2bd-f448a958b5be.png"/><image src="https://user-images.githubusercontent.com/448788/137563908-738a7431-88db-47b0-96a4-baaed7e5024b.png"/> |

<br/>

If you run `jmh.sh` with the `-lprof` argument, it will make an attempt to only list the profilers that it detects will work in your particular environment.

You should do this first to see where you stand.


<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610116-eff6d0b7-e862-40fb-af04-452aaf585387.png)

```Shell
./jmh.sh -lprof` 
```

</div>


<br/>

In our case, we will start with very **minimal** Arch and Ubuntu clean installations, and so we already know there is _**no chance**_ that async-profiler or Perfasm
are going to run.

In fact, first we have to install a few project build requirements before thinking too much about JMH profiler support.

We will run on **Arch/Manjaro**, but there should not be any difference than on **Debian/Ubuntu** for this stage.

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 5px 10px 10px;padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610116-eff6d0b7-e862-40fb-af04-452aaf585387.png)

```Shell
sudo pacman -S wget jdk-openjdk11
```

</div>

<br/>

Here we give **async-profiler** a try on **Arch** anyway and observe the failure indicating that we need to obtain the async-profiler library and
put it in the correct location at a minimum.

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">


```Shell
./jmh.sh BenchMark -prof async
```

<pre>
   <image src="https://user-images.githubusercontent.com/448788/137534191-01c2bc7a-5c1f-42a2-8d66-a5d1a5280db4.png"/>  Profilers failed to initialize, exiting.

    Unable to load async-profiler. Ensure asyncProfiler library is on LD_LIBRARY_PATH (Linux)
    DYLD_LIBRARY_PATH (Mac OS), or -Djava.library.path.

    Alternatively, point to explicit library location with: '-prof async:libPath={path}'

    no asyncProfiler in java.library.path: [/usr/java/packages/lib, /usr/lib64, /lib64, /lib, /usr/lib]
    </pre>

</div>

### Async-Profiler

#### Install async-profiler

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610116-eff6d0b7-e862-40fb-af04-452aaf585387.png)

```Shell
wget -c https://github.com/jvm-profiling-tools/async-profiler/releases/download/v2.5/async-profiler-2.5-linux-x64.tar.gz -O - | tar -xz
sudo mkdir -p /usr/java/packages/lib
sudo cp async-profiler-2.5-linux-x64/build/* /usr/java/packages/lib
```

</div>

<br/>

That should work out better, but there is still an issue that will prevent a successful profiling run. async-profiler relies on Linux's perf,
and in any recent Linux kernel, perf is restricted from doing its job without some configuration loosening.

Manjaro should have perf available, but you may need to install it in the other cases.

<br/>

![](https://user-images.githubusercontent.com/448788/137563908-738a7431-88db-47b0-96a4-baaed7e5024b.png)

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610566-883825b7-e66c-4d8b-a6a5-61542bc08d23.png)

```Shell
apt-get install linux-tools-common linux-tools-generic linux-tools-`uname -r`
```

</div>

<br/>

![](https://user-images.githubusercontent.com/448788/137563725-0195a732-da40-4c8b-a5e8-fd904a43bb79.png)

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610566-883825b7-e66c-4d8b-a6a5-61542bc08d23.png)

```Shell
pacman -S perf
```

</div>


<br/>

And now the permissions issue. The following changes will  persist across restarts, and that is likely how you should leave things.

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

```zsh
sudo sysctl -w kernel.kptr_restrict=0
sudo sysctl -w kernel.perf_event_paranoid=1
```

</div>

<br/>

Now we **should** see some success:

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610116-eff6d0b7-e862-40fb-af04-452aaf585387.png)

```Shell
./jmh.sh FuzzyQuery -prof async:output=flamegraph
```

</div>

<br/>

![](https://user-images.githubusercontent.com/448788/138650315-82adeb18-54cd-43ee-810e-24f1e22719c7.png)

<br/>

But you will also find an important _warning_ if you look closely at the logs.

<br/>

![](https://user-images.githubusercontent.com/448788/137613526-a188ff03-545c-465d-928d-bc433d2d204f.png)
<span style="color: yellow; margin-left: 5px;">[WARN]</span> `Install JVM debug symbols to improve profile accuracy`

<br/>

Ensuring that **Debug symbols** remain available provides the best experience for optimal profiling accuracy and heap-analysis.

And it also turns out that if we use async-profiler's **alloc** option to sample and create flamegraphs for heap usage, the **debug** symbols
are _required_.

<br/>

#### Install Java Debug Symbols

---

##### Ubuntu

![](https://user-images.githubusercontent.com/448788/137563908-738a7431-88db-47b0-96a4-baaed7e5024b.png)

Grab the debug package of OpenJdk using your package manager for the correct Java version.

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610566-883825b7-e66c-4d8b-a6a5-61542bc08d23.png)

```Shell
sudo apt update
sudo apt upgrade
sudo apt install openjdk-11-dbg
```

</div>

---

##### Arch

![](https://user-images.githubusercontent.com/448788/137563725-0195a732-da40-4c8b-a5e8-fd904a43bb79.png)

On the **Arch** side we will rebuild the Java 11 package, but turn off the option that strips debug symbols. Often, large OS package and Java repositories originated in SVN and can be a bit a of a bear to wrestle with git about for just a fraction
of the repository, we do so GitHub API workaround efficiency.

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610116-eff6d0b7-e862-40fb-af04-452aaf585387.png)

```Shell
sudo pacman -S dkms base-devel linux-headers dkms git vi jq --needed --noconfirm

curl -sL "https://api.github.com/repos/archlinux/svntogit-packages/contents/java11-openjdk/repos/extra-x86_64" \
| jq -r '.[] | .download_url' | xargs -n1 wget
```

</div>

<br/>

Now we need to change that option in PKGBUILD. Choose your favorite editor. (nano, vim, emacs, ne, nvim, tilde etc)

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610116-eff6d0b7-e862-40fb-af04-452aaf585387.png)

```Shell
vi PKGBUILD
```

</div>

<br/>

Insert a single option line:

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

```Diff
arch=('x86_64')
url='https://openjdk.java.net/'
license=('custom')
+   options=('debug' '!strip')
makedepends=('java-environment>=10' 'java-environment<12' 'cpio' 'unzip' 'zip' 'libelf' 'libcups' 'libx11' 'libxrender' 'libxtst' 'libxt' 'libxext' 'libxrandr' 'alsa-lib' 'pandoc'
```

</div>

<br/>

Then build and install. (`-s: --syncdeps -i: --install  -f: --force`)

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610116-eff6d0b7-e862-40fb-af04-452aaf585387.png)

```Shell
makepkg -sif
```

</div>

<br/>

When that is done, if everything went well, we should be able to successfully run async-profiler in alloc mode to generate a flame graph based on memory rather than cpu.

<br/>


<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610116-eff6d0b7-e862-40fb-af04-452aaf585387.png)

```Shell
./jmh.sh FuzzyQuery -prof async:output=flamegraph
```

</div>

<br/>

![](https://user-images.githubusercontent.com/448788/138661737-333bf265-343a-4002-b8a8-97d72c38ced0.png)

## Perfasm

Perfasm will run perf to collect hardware counter infromation (cycles by default) and it will also pass an argument to Java
to cause it to log assembly output (amoung other things). The performance data from perf is married with the assembly from the Java output log and Perfasm then does its thing to produce human parsable output. Java generally cannot output assembly as shipped however, so now
we must install **hsdis** to allow for `-XX+PrintAssembly`

* * *

### Arch

![](https://user-images.githubusercontent.com/448788/137563725-0195a732-da40-4c8b-a5e8-fd904a43bb79.png)

<br/>

[//]: # ( https://aur.archlinux.org/packages/java11-openjdk-hsdis/)

If you have `yay` or another **AUR** helper available, or if you have the **AUR** enabled in your package manager, simply install `java11-openjdk-hdis`

If you do not have simple access to **AUR**, set it up or just grab the package manually:

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610116-eff6d0b7-e862-40fb-af04-452aaf585387.png)

```Shell
wget -c https://aur.archlinux.org/cgit/aur.git/snapshot/java11-openjdk-hsdis.tar.gz -O - | tar -xz
cd java11-openjdk-hsdis/
makepkg -si
```

</div>

<br/>

---

### Ubuntu

![](https://user-images.githubusercontent.com/448788/137563908-738a7431-88db-47b0-96a4-baaed7e5024b.png)

<br/>

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610566-883825b7-e66c-4d8b-a6a5-61542bc08d23.png)

```Shell
sudo apt update
sudo apt -y upgrade
sudo apt -y install openjdk-11-jdk git wget jq
```

</div>

<br/>

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610566-883825b7-e66c-4d8b-a6a5-61542bc08d23.png)

```Shell
curl -sL "https://api.github.com/repos/openjdk/jdk11/contents/src/utils/hsdis" | jq -r '.[] | .download_url' | xargs -n1 wget

# Newer versions of binutils don't appear to compile, must use 2.28 for JDK 11
wget http://ftp.heanet.ie/mirrors/ftp.gnu.org/gnu/binutils/binutils-2.28.tar.gz
tar xzvf binutils-2.28.tar.gz
make BINUTILS=binutils-2.28 ARCH=amd64
```

</div>

<br/>

Now we should be able to do a little Perfasm:

<div style="z-index: 8;  background-color: #364850; border-style: solid; border-width: 1px; border-color: #3b4d56;border-radius: 0px; margin: 0px 5px 3px 10px; padding-bottom: 1px;padding-top: 5px;" data-code-wrap="true">

![](https://user-images.githubusercontent.com/448788/137610116-eff6d0b7-e862-40fb-af04-452aaf585387.png)

```Shell
./jmh.sh FuzzyQuery -prof perfasm
```

</div>
