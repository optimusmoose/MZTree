## System requirements:
- Build-time: Java 8 JDK, Apache Maven
- Runtime: Java 8 JRE or higher

## Build procedure
- Run `mvn package` from within the project root directory

## GUI start:
- Navigate to /msDataServer/target in a file manager
- Double click msDataServer-*version*.jar
  - You may need to make the .jar file executable first. This procedure varies depending on your operating system and file manager.
- A server window will open. From here, you may open a mass spectrometry data file and start the HTTP server and JS-MS.
- The msDataServer jar file can be run independently from the project folder as a standalone package

## Terminal/Command-line start:
- `java -jar /path/to/msDataServer-<version>.jar`
- It may be helpful in some cases to expand the maximum memory available to the application.
  This can be done by passing the `-Xmx` flag. For example, add `-Xmx8g` to give
  the application a maximum 8 gigabytes of memory.

# msDataServer Web API

###HTTP GET /api/v2/filestatus

Checks the status of the server's open-file process. Returns additional file data if a file has been loaded and data model is ready..

####Server response:

	HTTP 200 (OK): File has been loaded and ata model is ready, begin querying for points.
		Payload: { "mzmin" : float, "mzmax" : float, "rtmin" : float, "rtmax" : float, "intmin" : float, "intmax" : float, "pointcount": integer, "progress": float }
	HTTP 204 (No Content): No file has been selected, open a file before continuing.
	HTTP 400 (Bad Request): Malformed request (usually a missing parameter)
	HTTP 403 (Forbidden): The file is currently being selected
	HTTP 406 (Not Acceptable): The selected file is of the wrong file format, reselect file before continuing.
	HTTP 409 (Conflict): The server is selecting a file or processing the selected file. Continue checking file status.

###HTTP GET /api/v2/getpoints

Queries the database for points to display on the graph. Requests a specific number of points within the given bounds. Server determines the detail level that would provide the same or more points than requested and samples this set to the requested number of points.

####URL parameters:

	mzmin (double): mz lower bound (0 for global mz minimum)
	mzmax (double): mz upper bound (0 for global mz maximum)
	rtmin (float): rt lower bound (0 for global rt minimum)
	rtmax (float): rt upper bound (0 for global rt maximum)
	numpoints (int): the number of points to be returned (0 for no limit)

####Server response:

	HTTP 200 (OK): Query successfully serviced, returning points.
		Payload: [[<pointId>,<traceId>,<mz>,<rt>,<intensity>], ... ]
	HTTP 204 (No Content): No file has been selected, open a file before continuing.
	HTTP 400 (Bad Request): Malformed request, missing parameter or invalid query range (i.e. mzmin > mzmax).
	HTTP 406 (Not Acceptable): The previously selected file is of the wrong file format, reselect file before continuing.
	HTTP 409 (Conflict): The server is selecting a file or processing the selected file. Continue checking file status.
