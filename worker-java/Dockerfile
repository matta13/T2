# Usamos una imagen base de Java 11 JDK para la fase de compilación
FROM eclipse-temurin:11-jdk-jammy AS build

# Instalar Maven
RUN apt-get update && apt-get install -y maven

# Establecer directorio de trabajo
WORKDIR /app

# Copiar archivos de Maven y descargar dependencias
COPY worker-java/pom.xml .
RUN mvn dependency:go-offline

# Copiar el código fuente y compilar
COPY worker-java/src ./src
RUN mvn clean package

# Etapa de ejecución (usamos un JRE 11 más ligero)
FROM eclipse-temurin:11-jre-jammy
WORKDIR /app
# Copiar el JAR compilado de la etapa 'build'
COPY --from=build /app/target/gemini-worker-1.0-SNAPSHOT.jar ./app.jar

# Comando de ejecución del Worker
CMD ["java", "-jar", "app.jar"]