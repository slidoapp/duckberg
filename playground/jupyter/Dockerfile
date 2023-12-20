FROM jupyter/minimal-notebook

# Install Python Packages & Requirements (Done near end to avoid invalidating cache)
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Expose Jupyter port & cmd
EXPOSE 8888
CMD ["jupyter", "notebook", "--notebook-dir", "/opt/app/notebooks", "--no-browser","--NotebookApp.token=''","--NotebookApp.password=''"]