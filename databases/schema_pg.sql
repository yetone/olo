CREATE TABLE "dummy" (
  "id" serial NOT NULL,
  "name" varchar(255) NOT NULL,
  "age" integer NOT NULL,
  "password" varchar(512) DEFAULT NULL,
  "flag" smallint NOT NULL DEFAULT '0',
  "tags" text NOT NULL,
  "payload" text NOT NULL,
  "foo" integer DEFAULT NULL,
  "dynasty" varchar(4) DEFAULT NULL,
  "dynasty1" varchar(4) DEFAULT NULL,
  "created_at" timestamp NOT NULL,
  "updated_at" timestamp NOT NULL,
  "created_date" date NOT NULL,
  PRIMARY KEY ("id")
);

CREATE INDEX "idx_name_age" ON "dummy"("name", "age");

CREATE TABLE "foo" (
"id" serial NOT NULL,
"name" varchar(255) NOT NULL,
"age" integer NOT NULL,
"age_str" integer NOT NULL,
"key" varchar(255) NOT NULL,
PRIMARY KEY ("id"),
CONSTRAINT "key" UNIQUE ("key"),
CONSTRAINT "name-age" UNIQUE ("name", "age")
);

CREATE TABLE "__dummy" (
"id" serial NOT NULL,
"name" integer NOT NULL,
"age" integer NOT NULL,
"password" varchar(512) DEFAULT NULL,
"flag" smallint NOT NULL DEFAULT '0',
"tags" text NOT NULL,
"payload" text NOT NULL,
"foo" integer DEFAULT NULL,
"dynasty" varchar(4) DEFAULT NULL,
"dynasty1" varchar(4) DEFAULT NULL,
"created_at" timestamp NOT NULL,
"updated_at" timestamp NOT NULL,
"created_date" date NOT NULL,
PRIMARY KEY ("id")
);

CREATE TABLE "bar" (
"name" varchar(255) NOT NULL,
"age" integer NOT NULL,
"key" varchar(255) NOT NULL,
"word" varchar(255),
PRIMARY KEY ("name")
);

CREATE TABLE "ttt" (
"id" serial NOT NULL,
"created_at" timestamp NOT NULL,
PRIMARY KEY ("id")
);

CREATE TABLE "cool" (
"name" varchar(255) NOT NULL,
"age" integer NOT NULL,
"key" varchar(255) NOT NULL,
PRIMARY KEY ("name", "age")
);

CREATE TABLE "lala" (
"id" serial NOT NULL,
"name" varchar(255) NOT NULL,
"age" integer NOT NULL,
PRIMARY KEY ("id"),
CONSTRAINT "uk_name_age" UNIQUE ("name", "age")
);
