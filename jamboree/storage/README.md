# Storage Models

The storage model will present a common interface for all common queries and write commands. They will be separated into two parts:

1. Files (TBA)
   * This will be for everything related to file management. The central idea behind it is that we'll be able to store gigabyte to terrabyte sized files into cloud platforms, such as S3 & DataLake. 
   * We'll also have procedures to store information into memory, such as redis. We'll split the files at a higher level so they can be better handled.
2. Database Connection. 
   * Since the main Jamboree object is starting to become bloated, the main goal here is to create something that would allow us to run through different datastores with little to no problem. 
   * We're starting with `mongodb` and `redis`, but with the abstracts available we'll be able to move into other data stores as well.