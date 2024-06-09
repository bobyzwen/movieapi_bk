package com.bwen.movieapi.service;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.bwen.movieapi.dto.Movie;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

@Log4j2
@Service
public class S3Service {
    private static final String ACCESS_KEY = System.getenv("S3_ACCESS_KEY"); //"0000AKIA2UC3CIXA35EBRTEK";
    private static final String ACCESS_KEY_SEC = System.getenv("S3_ACCESS_SECRET"); // "0000HizcXMyF1q+CtD0cDK06pG2MeRu+A2s652xYrR/N";
    private static final String BUCKET_NAME = "defaultawsbucket";
    private static final String JSON_OBJECT_KEY = "movies/movies.json";
    private static final String QUERY_BY_NAME = "select * from S3Object[*][*] s where lower(s.title) ";
    private static final String QUERY_BY_YEAR = "select * from S3Object[*][*] s where s.\"year\" = ";
    final private AmazonS3 s3Client;
    final private ObjectMapper objectMapper;

    public S3Service() {
        this.objectMapper = new ObjectMapper();
        this.s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, ACCESS_KEY_SEC)))
                .withRegion(Regions.EU_WEST_2)
                .build();;
    }
    public List<Movie> queryMoviesByNameAndYear(String name, int year){
        String queryString =  QUERY_BY_YEAR  + year + " and lower(s.title) like '%" + name.toLowerCase() + "%'";
        return queryMovies(queryString);
    }
    public List<Movie> queryMoviesByName(String name, boolean exactMatch){
        String queryString = QUERY_BY_NAME +  (exactMatch ? "= '" + name.toLowerCase() + "'" : "like '%" + name.toLowerCase() + "%'");
        return queryMovies(queryString);
    }

    public List<Movie> queryMoviesByYear(int year){
        String queryString = QUERY_BY_YEAR  + year;
        return queryMovies(queryString);
    }

    private List<Movie> queryMovies(String queryString) {
        log.info("Executing Query in S3 Select: {}", queryString);
        List<Movie> movies;
        try (SelectObjectContentResult result = s3Client.selectObjectContent(generateJsonQueryRequest(BUCKET_NAME, JSON_OBJECT_KEY, queryString))) {
            InputStream inputStream = result.getPayload().getRecordsInputStream();
            movies = new BufferedReader(new InputStreamReader(inputStream))
                    .lines()
                    .map(this::mapJsonToMovie)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("Query completed, result size: {}", movies.size());
        return movies;
    }

    private Movie mapJsonToMovie(String jsonString){
        try {
            return objectMapper.readValue(jsonString, Movie.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
    private SelectObjectContentRequest generateJsonQueryRequest(String bucket, String key, String query) {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(bucket);
        request.setKey(key);
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);

        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setJson(new JSONInput().withType("Document"));
        inputSerialization.setCompressionType(CompressionType.NONE);
        request.setInputSerialization(inputSerialization);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setJson(new JSONOutput().withRecordDelimiter("\n"));
        request.setOutputSerialization(outputSerialization);

        return request;
    }
}
