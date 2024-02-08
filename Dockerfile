FROM openjdk:19-jdk-alpine
COPY target/scala-2.13/dr2-disaster-recovery.jar /dr2-disaster-recovery.jar
RUN mkdir -p /poduser/work /poduser/repo && chown -R 1002:1005 /poduser
USER 1002
CMD java -jar /dr2-disaster-recovery.jar
