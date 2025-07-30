FROM python:3.12.3

#Setting the working directory within the container
WORKDIR /app

# Copy everything from the current directory into /app
COPY . /app

RUN pip install -r requirements.txt

# On starting run python with demo-level1.py
ENTRYPOINT ["python","demo_level1.py"]