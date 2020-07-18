def process():
    !echo Y | apt - get
    !purge openjdk - 11 - jre openjdk - 11 - jre - headless
    !echo Y | apt install openjdk - 8 - jdk
    !pip install pyspark

process()
