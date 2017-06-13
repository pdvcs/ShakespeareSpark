# ShakespeareAnalytics

A starter Java 8 application with Apache Spark, built using Java 8.

## Compiling and Running

Compile using `mvn package`. This produces an uberjar. (**TODO**: exclude unnecessary items from the uberjar.)

Use the following command line to run:
```
Usage: ShakespeareAnalytics /path/to/input-file top-N-above-5-letters
 e.g.: java -jar ShakespeareAnalytics-0.0.1-SNAPSHOT.jar /home/you/ws_cleaned.txt 100
```

## Input Data

* Get the plain-text version of [The Complete Works of Shakespeare](https://www.gutenberg.org/ebooks/100) from Project Gutenberg.
* Remove the license header and footer using a text editor.
* Use `sed` to remove the inline licence blocks, thus:

```
$ sed '/<<THIS ELECTRONIC VERSION OF THE COMPLETE WORKS OF WILLIAM/,/SERVICE THAT CHARGES FOR DOWNLOAD TIME OR FOR MEMBERSHIP.>>/d' shakespeare.txt > ws_cleaned.txt 
```
