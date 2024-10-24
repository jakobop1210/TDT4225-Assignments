# Assignment 3

To start MongoDB sever run:

```bash
docker-compose up -d
```

To shut down server run:

```bash
docker-compose down
```

You can then access the database at `mongodb://root:example@localhost:27017?authSource=admin` using a MongoDB client or application, like MongoDB Compass.

### Setup python

To install the required python packages run:

```bash
pip install -r requirements.txt
```

### Use of dataset
The dataset should be organized in the following structure inside the Assignment3 directory:

```bash
Assignment3/
│
└───dataset/
    ├───dataset/
    │   ├───labeled_ids.txt
    │   └───Data/
    │       ├───User_001/
    │       ├───User_002/
    │       ├───User_003/
    │       └───...
```