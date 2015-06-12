
jFAST
=====

[FIX Adapted for STreaming optimized for the JVM ](http://en.wikipedia.org/wiki/FAST_protocol)


This project is under active development.
Please consider getting involved and sponsoring the completion of [jFAST](mailto:info@ociweb.com;?subject=jFAST%20Sponsor%20Inquiry)
                 

# Design Goals

* Fastest FAST protocol implementation for the Java platform.
* Garbage free implementation.
* Branch minimization for consistent (deterministic) performance.
* Restricted to Java 6/7 for Android compatibility

# Run Demo

  mvn install
  
  cd target
  
  java -jar jFAST.jar

    
# Add to Maven project
  
  To use this in your maven project add the following dependency.
  
    <dependency>
      <groupId>com.ociweb</groupId>
      <artifactId>jFAST</artifactId>
      <version>0.0.1-SNAPSHOT</version>
    </dependency>
   
  Also add this public repository to your pom or settings.

    <repository>
      <releases>
        <enabled>false</enabled>
      </releases>
      <snapshots>
        <enabled>true</enabled>
      </snapshots>
      <id>repository-pronghorn.forge.cloudbees.com</id>
      <name>Active Repo for PronghornPipes</name>
      <url>http://repository-pronghorn.forge.cloudbees.com/snapshot/</url>
      <layout>default</layout>
    </repository>




# Code example
  
  Review code found in com.ociweb.jfast.TestApp and com.ociweb.jfast.example.UsageExample
 
