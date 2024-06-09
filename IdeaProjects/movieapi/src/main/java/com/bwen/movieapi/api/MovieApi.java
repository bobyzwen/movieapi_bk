package com.bwen.movieapi.api;

import com.bwen.movieapi.dto.Movie;
import com.bwen.movieapi.service.S3Service;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Log4j2
@RestController
public class MovieApi {
    final private S3Service s3Service;

    public MovieApi(S3Service s3Service) {
        this.s3Service = s3Service;
    }

    @GetMapping("/api/movies")
    public ResponseEntity<List<Movie>> query(@RequestParam(required = false) String name, @RequestParam(required = false) Integer year){
        log.info("Received query for movie: name={}, year={}",name,year);
        if(StringUtils.isEmpty(name) && year == null){
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Query movies either by Name or Year");
        }
        List<Movie> result;
        if( !StringUtils.isEmpty(name) && year != null ){
            result = s3Service.queryMoviesByNameAndYear(name, year);
        }else if(! StringUtils.isEmpty(name)) {
            result = s3Service.queryMoviesByName(name, false);
        }else {
            result = s3Service.queryMoviesByYear(year);
        }
        if(result == null || result.size() == 0){
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No result found with query parameters");
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }
}

