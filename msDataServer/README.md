# MZTree
A visualization and segmentation HTTP interface for Mass Spectrometry signal data. Provides efficient, axis-agnostic data range querying and storage of additional signal attributes. Interfaces via an HTTP API able to communicate with any JSON-fluent client.

## Dependencies
* Java 8

## Usage
1. Run msDataServer-1.0.jar
	 Navigate to jar and double click OR run command: `java -jar msDataServer-1.0.jar`
    
2. Enter desired port number
3. Click "Start Server"
4. Click "Open File"
5. Select *mzML* or *MzTree* file
6. Begin interacting with MsDataServer via HTTP (see API Specifications)

## Build
MzTree is managed and built via Maven. To build [download Maven](https://maven.apache.org/download.cgi) and [set up your Maven environment](https://www.tutorialspoint.com/maven/maven_environment_setup.htm). Then, with *pom.xml* visible, run the command `mvn install`

Alternatively, you can build with NetBeans IDE. Download [NetBeans](https://netbeans.org/downloads/), open the *msDataServer* project and use the NetBeans build tool (located in the toolbar) to build.

## API Specifications

*HTTP GET /api/v1/openfile*
		
Prompts the data server to display a file chooser dialog
	
	Server response:
		HTTP 200 (OK): Server has begun opening file, OK to ping server for file status.
		HTTP 403 (Forbidden): Server is already opening file.

-

*HTTP GET /api/v1/filestatus*
		
Sent periodically to the server to ask if a file is done processing.

	Server response:
		HTTP 200 (OK): Data model is ready, begin querying for points.
			Payload: { "mzmin" : float, "mzmax" : float, "rtmin" : float, "rtmax" : float, "intmin" : float, "intmax" : float }
		HTTP 204 (No Content): No file has been selected, open a file before continuing.
		HTTP 400 (Bad Request): Malformed request (usually a missing parameter)
		HTTP 403 (Forbidden): The file is currently being selected
		HTTP 406 (Not Acceptable): The selected file is of the wrong file format, reselect file before continuing 
		HTTP 409 (Conflict): The server is selecting a file or processing the selected file. Continue checking file status.
 		

-

*HTTP GET /api/v1/getpoints*
		
Queries the database for points to display on the graph. Requests a specific 
		number of points within the given bounds. Server determines the detail level
		that would provide the same or more points than requested and samples this set
		to the requested number of points. Additionally, returns segmentation assignments in the form of traceID->envelopeID mappings.

	Parameters:
		mzmin: mz lower bound (0 for global mz minimum)
		mzmax: mz upper bound (0 for global mz maximum)
		rtmin: rt lower bound (0 for global rt minimum)
		rtmax: rt upper bound (0 for global rt maximum)
		numpoints: the number of points to be returned (0 for no limit)
		intmin: minimum intensity criterion for returned points 

	Server response:
		HTTP 200 (OK): Query successfully serviced, returning points and traceMap
			Payload: { "points": [(<mz>,<rt>,<intensity>), ... ], "traceMap": {"<traceID>":<envID>, ... } }
		HTTP 204 (No Content): No file has been selected, open a file before continuing.
		HTTP 400 (Bad Request): Malformed request -> missing parameter or invalid query range (i.e. mzmin > mzmax)
		HTTP 406 (Not Acceptable): The previously selected file is of the wrong file format, reselect file before continuing
		HTTP 409 (Conflict): The server is selecting a file or processing the selected file. Continue checking file status.

-

*HTTP GET /api/v1/savemodel*
		
Prompts the server to save the currently loaded mzTree as a .mzTree file (sqlite database)

	Parameters:
		filename: name to give the output file

	Server response:
		HTTP 200 (OK): Model successfully saved as csv file
			Payload: { filepath: path-to-file }
		HTTP 204 (No Content): No data model exists
		HTTP 409 (Conflict): A save with the filename already exists

-

*HTTP POST /api/v1/updatesegmentation*
		
	Sends modified segmentation data to the server to update the mzTree

	POST request parameters (JSON):
                [ <action>, <action>, ... ]
        Action format - one of following:
		{ "type": "undo" }
		{ "type": "redo" }
                { "type": "set-trace", "trace": <traceid>, "points": [<pointid>, ...] }
                { "type": "set-envelope", "envelope": <envelopeid>, "traces": [<traceid>, ...] }
                { "type": "rectangle", "bounds": [<lower_mz>, <upper_mz>, <lower_rt>, <upper_rt>], "id": <traceID>, "isAdd": <boolean> }

	Server response:
		HTTP 200 (OK): Successfully updated segmentation data
		HTTP 204 (NO_CONTENT): No data model exists
		HTTP 500 (INTERNAL_SERVER_ERROR): Failed update segmentation data


## License
This work is published under the Gnu General Public License (GPL) v2. Please see the LICENSE file at the root level of this repository for more details.

## Notice 
Elements of this software are patent-pending. For commercial license opportunities, contact Dr. Rob Smith at robert.smith@mso.umt.edu.
