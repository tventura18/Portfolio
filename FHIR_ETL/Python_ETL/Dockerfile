# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

# Default command to run your pipeline
#CMD ["python", "EOBS_etl.py"]
CMD ["python", "eob_items_coverage.py"]
#CMD ["bash", "-c", "jupyter nbconvert --to notebook --execute /app/your_notebook.ipynb --output /app/output_executed.ipynb --ExecutePreprocessor.timeout=-1"]
#CMD ["bash", "-c", "jupyter nbconvert --to notebook --execute /app/eob_items_coverage.ipynb --output /app/output_executed.ipynb --ExecutePreprocessor.timeout=-1"]
