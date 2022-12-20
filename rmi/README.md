# RMI

## QuickStart

1. Check files
    ```bash
    tree
    .
    ├── README.md
    └── example
        └── hello
            ├── Client.java
            ├── Hello.java
            └── Server.java

    2 directories, 4 files
    ```
1. Compile java files.
    ```
    javac example/hello/*.java
    ```

1. Run rmiregistry.
    ```
    rmiregistry
    ```
1. Run RMI Server.

    ```
    java example.hello.Server
    ```

1. Run client.

    ```
    java example.hello.Client
    response:Hello, World!
    ```

1. Run with Jar

    1. Prepare `META-INF/MANIFEST.MF`

        ```
        Main-Class: example.hello.Server
        ```
    1. Make a jar

        ```
        jar cvfm example-hello.jar META-INF/MANIFEST.MF example/hello/*.class
        added manifest
        adding: example/hello/Client.class(in = 1484) (out= 797)(deflated 46%)
        adding: example/hello/Hello.class(in = 251) (out= 190)(deflated 24%)
        adding: example/hello/Server.class(in = 1594) (out= 859)(deflated 46%)
        ```
    1. Run RMI registry (You can skip this step if you use `createRegistry()` instead of `getRegistry()`)
        ```
        rmiregistry
        ```
    1. Run server.
        ```
        java -jar example-hello.jar
        ```
    1. Run client.
        ```
        java -cp example-hello.jar example.hello.Client
        response:Hello, World!
        ```

## Error:

1. `java.lang.ClassNotFoundException: example.hello.Hello`: this is because rmiregistry cannot find `Hello` class. You need to either run rmiregistry in the `rmi` directory or pass the class path to `rmiregistry`.
    ```
    java example.hello.Server
    Server exception: java.rmi.ServerException: RemoteException occurred in server thread; nested exception is:
            java.rmi.UnmarshalException: error unmarshalling arguments; nested exception is:
            java.lang.ClassNotFoundException: example.hello.Hello
    ```
1. `java.rmi.ConnectException: Connection refused to host: 192.168.10.51; nested exception is: java.net.ConnectException: Connection refused`: this is because rmiregistry is not running. You need to either run rmiregistry or change `getRegistry()` to `createRegistry(1099)`.

## References
1. https://www.baeldung.com/java-could-not-find-load-main-class
1. https://docs.oracle.com/javase/jp/1.5.0/guide/rmi/hello/hello-world.html
1. https://stackoverflow.com/questions/14071885/java-rmi-unmarshalexception-error-unmarshalling-arguments-nested-exception-is
