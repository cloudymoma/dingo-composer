composer:
	gcloud composer environments create dingo-composer \
		--location us-central1 \
		--image-version composer-3-airflow-2 \
		--service-account bindiego@du-hast-mich.iam.gserviceaccount.com \
		--environment-size small

deploy:
	./deploy.sh

.PHONY: composer deploy
