#!/usr/bin/python3
import shutil
import subprocess

spark_versions = [
    "2.0.0", "2.0.1", "2.0.2",
    "2.1.0",
    "2.1.1", "2.1.2", "2.1.3",
    "2.2.0", "2.2.1", "2.2.2", "2.2.3",
    "2.3.0", "2.3.1", "2.3.2", "2.3.3",
    "2.4.0", "2.4.1", "2.4.2", "2.4.3", "2.4.4", "2.4.5",
    "3.0.0", "3.0.1", "3.0.2", "3.0.3",
    "3.1.0", "3.1.1", "3.1.2", "3.1.3",
    "3.2.0"]

for spark_version in spark_versions:
    print("Next spark version " + spark_version)
    print("\nbuilding\n")
    print(f"\nGoing to run: ./sbt/sbt  -DsparkVersion={spark_version} version clean +publishSigned\n")
    subprocess.run(f"sbt -DsparkVersion={spark_version} version clean +publishSigned sonatypeBundleRelease", shell=True, check=True)
    print("\nbuilt\n")
