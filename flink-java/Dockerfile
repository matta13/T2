# Usamos una imagen base de Java 11 JDK para la fase de compilación
FROM eclipse-temurin:11-jdk-jammy AS build 

# Instalar Maven
RUN apt-get update && apt-get install -y maven netcat-openbsd
# Establecer directorio de trabajo
WORKDIR /app

# Copiar archivos de Maven y descargar dependencias
COPY flink-java/pom.xml .
RUN mvn dependency:go-offline

# Copiar el código fuente y compilar
COPY flink-java/src ./src
RUN mvn clean package

# Etapa de ejecución final
# Usamos la imagen base de Flink compatible con Java 11
FROM flink:1.17-scala_2.12
WORKDIR /opt/flink/usrlib/

# Copiar el JAR compilado de la etapa 'build'
COPY --from=build /app/target/flink-qa-job.jar ./