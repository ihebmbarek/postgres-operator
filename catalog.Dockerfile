FROM quay.io/operator-framework/opm:latest
COPY catalog /configs
ENTRYPOINT ["/bin/opm"]
CMD ["serve", "/configs"]
