# base Docker image that we will build on
FROM python:3.9.1

# set up the working directory inside the container
WORKDIR /app

# Due to WORKDIR, now we are inside /app inside the container.
# Let us copy the requirements.txt and the python script we want to run
# inside the container. 1st name is source file, 2nd is destination
COPY ingest_data.py ingest_data.py
COPY requirements.txt requirements.txt

# set up our image by installing prerequisites; pandas in this case
RUN pip install -r requirements.txt

# define what to do first when the container runs
# in this example, we will just run the script
ENTRYPOINT ["python", "ingest_data.py"]

###
# How to use this Dockerfile
# 1. How to build a Docker image called "ingestdata" from this Dockerfile
# $ docker build -t ingestdata .
# 2. Running the pipeline.py inside the built image
# $ docker run -it ingestdata 
# 3. Getting into a bash shell (to install or inspect the image) instead of running pipeline.py
# $ docker run -it --entrypoint "bash" ingestdata
###