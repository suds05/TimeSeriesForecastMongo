
# Choose your desired base image
FROM jupyter/pyspark-notebook:latest

# Switch to root for customizations requiring root caccess later
# TODO: Not working. Check later
# ARG GRANT_SUDO=yes

# name your environment and choose the python version
ARG conda_env=vscode_pyspark
ARG py_ver=3.11

# you can add additional libraries you want mamba to install by listing them below the first line and ending with "&& \"
RUN mamba create --yes -p "${CONDA_DIR}/envs/${conda_env}" python=${py_ver} ipython ipykernel && \
    mamba clean --all -f -y

# alternatively, you can comment out the lines above and uncomment those below
# if you'd prefer to use a YAML file present in the docker build context

# COPY --chown=${NB_UID}:${NB_GID} environment.yml "/home/${NB_USER}/tmp/"
# RUN cd "/home/${NB_USER}/tmp/" && \
#     mamba env create -p "${CONDA_DIR}/envs/${conda_env}" -f environment.yml && \
#     mamba clean --all -f -y

# create Python kernel and link it to jupyter
RUN "${CONDA_DIR}/envs/${conda_env}/bin/python" -m ipykernel install --user --name="${conda_env}" && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

# any additional pip installs can be added by uncommenting the following line
RUN "${CONDA_DIR}/envs/${conda_env}/bin/pip" install pyspark pandas python-dotenv --no-cache-dir

# if you want this environment to be the default one, uncomment the following line:
RUN echo "conda activate ${conda_env}" >> "${HOME}/.bashrc"

# Download any JARS here. Right now, the mongo-spark-connector can be pulled directly by Spark Job. So not needed

# # Chage to root to download JARS
# USER root
# # Download the jars
# RUN wget -qO /usr/local/spark/jars/mongo-spark-connector_2.12-10.4.0jar https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.4.0/mongo-spark-connector_2.12-10.4.0.jar
# RUN wget -qO /usr/local/spark/jars/mongodb-driver-sync-5.2.1.jar https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.2.1/mongodb-driver-sync-5.2.1.jar
# # Switch back to jovyan to avoid accidental container runs as root
# USER jovyan