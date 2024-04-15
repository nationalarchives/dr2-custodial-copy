FROM openjdk:19-jdk-alpine
COPY target/scala-3.3.3/dr2-disaster-recovery.jar /dr2-disaster-recovery.jar
RUN mkdir -p /poduser/work /poduser/repo && \
    chown -R 1002:1005 /poduser && \
    mkdir /poduser/logs && \
    touch /poduser/logs/disaster-recovery.log && \
    chown -R nobody:nobody /poduser/logs && \
    chmod 644 /poduser/logs/disaster-recovery.log
USER 1002
RUN ls -la /poduser/logs
CMD java -jar /dr2-disaster-recovery.jar
