# Assignment 2

To start MySQL sever run:

```bash
docker-compose up -d
```

To shut down server run:

```bash
docker-compose down
```

Go to [http://localhost:8080/](http://localhost:8080/) to see the database

MySql credentials:

- server: db
- username: root
- password: group86

### Use of dataset
The dataset should be organized in the following structure inside the Assignment2 directory:

```bash
Assignment2/
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