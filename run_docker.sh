docker build -t work/pushkin_card_recommender_system -f ./Dockerfile . && \
docker run --rm -it \
  -v $(pwd)/app:/app -v $(pwd)/data:/data \
  work/pushkin_card_recommender_system bash run.sh