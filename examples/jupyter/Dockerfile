FROM tabulario/spark-iceberg

# Install Python Packages & Requirements (Done near end to avoid invalidating cache)
COPY requirements.txt requirements-duckberg.txt
RUN pip install -r requirements-duckberg.txt

# Expose Jupyter port & cmd
EXPOSE 8888
CMD ["jupyter", "notebook", "--notebook-dir", "/opt/app/notebooks", "--no-browser","--NotebookApp.token=''","--NotebookApp.password=''"]