DROP TABLE "data".recommendation_logs;

CREATE TABLE "data".recommendation_logs (
	weights json NULL,
	primary_interests json NULL,
	"timestamp" timestamp NOT NULL DEFAULT now()
);