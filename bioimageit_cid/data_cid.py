# -*- coding: utf-8 -*-
"""BioImageIT CID metadata service.

This module implements the CID service for metadata
(Data, DataSet and Experiment) management.
This CID service read/write and query metadata from an CID database

Classes
------- 
CIDMetadataServiceBuilder
CIDMetadataService

"""
import numpy as np
import os
import os.path
import json
import requests

from bioimageit_formats import FormatsAccess, formatsServices

from bioimageit_core.core.config import ConfigAccess
from bioimageit_core.core.exceptions import DataServiceError
from bioimageit_core.containers.data_containers import (METADATA_TYPE_RAW,
                                                        METADATA_TYPE_PROCESSED,
                                                        Container,
                                                        RawData,
                                                        ProcessedData,
                                                        ProcessedDataInputContainer,
                                                        Dataset,
                                                        Experiment,
                                                        Run,
                                                        RunInputContainer,
                                                        RunParameterContainer,
                                                        DatasetInfo,
                                                        )



plugin_info = {
    'name': 'CID',
    'type': 'data',
    'builder': 'CIDMetadataServiceBuilder'
}


class CIDMetadataServiceBuilder:
    """Service builder for the metadata service"""

    def __init__(self):
        self._instance = None

    def __call__(self, host, username, password):
        if not self._instance:
            self._instance = CIDMetadataService(host, username, password)
        return self._instance


class CIDMetadataService:
    """Service for local metadata management"""

    def __init__(self, host, username, password):
        self.service_name = 'CIDMetadataService'
        self._host = host
        self._username = username
        self._password = password
        self.token = None
        self._cid_connect()        

    def _send_request(self, url, verb, params, use_token=True):
        """Send a REST request using the requests library

        Parameters
        ----------
        url: str
            URL of the request without the API root URL. Ex: authenticate.php
        verb: str
            Request verb (GET, POST, DELETE)  
        params: dict
            value of the parameter in the request
        use_token: bool          
            True to add an Authorization header in the request 
        
        """
        url = f"{self._host}/{url}"
        
        # instantiate request
        method_map = {
            'GET': (requests.get, {}),
            'POST': (requests.post, {}),
            'DELETE': (requests.delete, {}),
        }
        requests_method, headers = method_map[verb]
        if use_token:
            headers.update({'Authorization': self.token})
        kwargs = {'headers': headers, 'data': params}

        # run the request
        req = requests_method(url, **kwargs)

        # check if error
        if not req.ok:
            raise DataServiceError(f'CID communication error: {req.status_code}')

        if req.status_code == 204:
            return True

        return req.json        


    def _cid_connect(self):
        """Get the session token"""
        print('CID connect')

        params = {'username': self._username, 'password': self._password}
        res = self._send_request('authenticate.php', 'POST', params, use_token=False)

        if 'httpHeaderValue' in res:
            self.token = res['httpHeaderValue']
        else:
            raise DataServiceError(
                'Unable to connect to the CID database'
            )

    def needs_cleanning(self):
        return True

    def create_experiment(self, name, author, date='now', keys=None,
                          destination=''):
        """Create a new experiment

        Parameters
        ----------
        name: str
            Name of the experiment
        author: str
            username of the experiment author
        date: str
            Creation date of the experiment
        keys: list
            List of keys used for the experiment vocabulary
        destination: str
            Destination where the experiment is created. It is a the path of the
            directory where the experiment will be created for local use case

        Returns
        -------
        Experiment container with the experiment metadata

        """
        raise NotImplementedError() 

    def get_workspace_experiments(self, workspace_uri = ''):
        """Read the experiments in the user workspace

        Parameters
        ----------
        workspace_uri: str
            URI of the workspace

        Returns
        -------
        list of experiment containers  
          
        """
        raise NotImplementedError()   

    def get_experiment(self, md_uri):
        """Read an experiment from the database

        Parameters
        ----------
        md_uri: str
            URI of the experiment. For local use case, the URI is either the
            path of the experiment directory, or the path of the
            experiment.md.json file

        Returns
        -------
        Experiment container with the experiment metadata

        """

        params= {"action": "project",
	             "parameter": "id_project",
	             "value": md_uri
                }

        params = {'username': self._username, 'password': self._password}
        res = self._send_request('get_data.php', 'GET', params)

        container = Experiment()
        if 'projects' in res and len(res['projects']) > 0:
            project = res['projects']
            container.uuid = project['id']    
            container.md_uri = project['id']
            container.name = project['label']
            container.author = project['owner']
            container.date = project['date']

            # TODO add request to get the list of datasets in this project
            
        else:
            raise DataServiceError(
                f'Unable to find the experiment {md_uri}'
            )
        return container

    def update_experiment(self, experiment):
        """Write an experiment to the database

        Parameters
        ----------
        experiment: Experiment
            Container of the experiment metadata

        """
        raise NotImplementedError()   

    def import_data(self, experiment, data_path, name, author, format_,
                    date='now', key_value_pairs=dict):
        """import one data to the experiment

        The data is imported to the raw dataset

        Parameters
        ----------
        experiment: Experiment
            Container of the experiment metadata
        data_path: str
            Path of the accessible data on your local computer
        name: str
            Name of the data
        author: str
            Person who created the data
        format_: str
            Format of the data (ex: tif)
        date: str
            Date when the data where created
        key_value_pairs: dict
            Dictionary {key:value, key:value} to annotate files

        Returns
        -------
        class RawData containing the metadata

        """
        raise NotImplementedError()   

    def import_dir(self, experiment, dir_uri, filter_, author, format_, date,
                   directory_tag_key='', observers=None):
        """Import data from a directory to the experiment

        This method import with or without copy data contained
        in a local folder into an experiment. Imported data are
        considered as RawData for the experiment

        Parameters
        ----------
        experiment: Experiment
            Container of the experiment metadata
        dir_uri: str
            URI of the directory containing the data to be imported
        filter_: str
            Regular expression to filter which files in the folder
            to import
        author: str
            Name of the person who created the data
        format_: str
            Format of the image (ex: tif)
        date: str
            Date when the data where created
        directory_tag_key
            If the string directory_tag_key is not empty, a new tag key entry with the
            key={directory_tag_key} and the value={the directory name}.
        observers: list
            List of observers to notify the progress

        """
        raise NotImplementedError()                                  

    def get_raw_data(self, md_uri):
        """Read a raw data from the database

        Parameters
        ----------
        md_uri: str
            URI if the raw data
        Returns
        -------
        RawData object containing the raw data metadata

        """
        raise NotImplementedError()   

    def update_raw_data(self, raw_data):
        """Read a raw data from the database

        Parameters
        ----------
        raw_data: RawData
            Container with the raw data metadata

        """
        raise NotImplementedError()   
       

    def get_processed_data(self, md_uri):
        """Read a processed data from the database

        Parameters
        ----------
        md_uri: str
            URI if the processed data

        Returns
        -------
        ProcessedData object containing the raw data metadata

        """
        raise NotImplementedError()   

    def update_processed_data(self, processed_data):
        """Read a processed data from the database

        Parameters
        ----------
        processed_data: ProcessedData
            Container with the processed data metadata

        """
        raise NotImplementedError()   

    def get_dataset(self, md_uri):
        """Read a dataset from the database using it URI

        Parameters
        ----------
        md_uri: str
            URI if the dataset

        Returns
        -------
        Dataset object containing the dataset metadata

        """
        raise NotImplementedError()      

    def update_dataset(self, dataset):
        """Read a processed data from the database

        Parameters
        ----------
        dataset: Dataset
            Container with the dataset metadata

        """
        raise NotImplementedError()   

    def create_dataset(self, experiment, dataset_name):
        """Create a processed dataset in an experiment

        Parameters
        ----------
        experiment: Experiment
            Object containing the experiment metadata
        dataset_name: str
            Name of the dataset

        Returns
        -------
        Dataset object containing the new dataset metadata

        """
        raise NotImplementedError()   

    def create_run(self, dataset, run_info):
        """Create a new run metadata

        Parameters
        ----------
        dataset: Dataset
            Object of the dataset metadata
        run_info: Run
            Object containing the metadata of the run. md_uri is ignored and
            created automatically by this method

        Returns
        -------
        Run object with the metadata and the new created md_uri

        """
        raise NotImplementedError()   

    def get_dataset_runs(self, dataset):
        """Read the run metadata from a dataset

        Parameters
        ----------
        dataset: Dataset

        Returns
        -------
        List of Runs

        """
        raise NotImplementedError()             

    def get_run(self, md_uri):
        """Read a run metadata from the data base

        Parameters
        ----------
        md_uri
            URI of the run entry in the database

        Returns
        -------
        Run: object containing the run metadata

        """
        raise NotImplementedError()   

 
    def get_data_uri(self, data_container):
        workspace = ConfigAccess.instance().config['workspace']
        extension = FormatsAccess.instance().get(data_container.format).extension
        destination_input = os.path.join(workspace,f"{data_container.name}.{extension}")
        return destination_input

    def create_data_uri(self, dataset, run, processed_data):
        workspace = ConfigAccess.instance().config['workspace']

        extension = FormatsAccess.instance().get(processed_data.format).extension
        processed_data.uri = os.path.join(workspace, f"{processed_data.name}.{extension}")
        return processed_data

    def create_data(self, dataset, run, processed_data):
        """Create a new processed data for a given dataset

        Parameters
        ----------
        dataset: Dataset
            Object of the dataset metadata
        run: Run
            Metadata of the run
        processed_data: ProcessedData
            Object containing the new processed data. md_uri is ignored and
            created automatically by this method

        Returns
        -------
        ProcessedData object with the metadata and the new created md_uri

        """
        raise NotImplementedError()       

    def download_data(self, md_uri, destination_file_uri):
        raise NotImplementedError()   
