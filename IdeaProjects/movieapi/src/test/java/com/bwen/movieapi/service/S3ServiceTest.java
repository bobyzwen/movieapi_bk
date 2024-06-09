package com.bwen.movieapi.service;

import com.bwen.movieapi.dto.Movie;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


class S3ServiceTest {
    S3Service s3Service = new S3Service();

    @Test
    void queryMoviesByName() {
        List<Movie> movieList = s3Service.queryMoviesByName("avatar", false);
        assertNotNull(movieList);
        assertEquals(3, movieList.size());
    }

    @Test
    void queryMoviesByNameExactMatch() {
        List<Movie> movieList = s3Service.queryMoviesByName("avatar", true);
        assertNotNull(movieList);
        assertEquals(1, movieList.size());
    }

    @Test
    void queryMoviesByNameAndYear() {
        List<Movie> movieList = s3Service.queryMoviesByNameAndYear("avatar", 2022);
        assertNotNull(movieList);
        assertEquals(1, movieList.size());
    }

    @Test
    void queryMoviesByYear() {
        List<Movie> movieList =  s3Service.queryMoviesByYear(2019);
        assertNotNull(movieList);
        assertEquals(245, movieList.size());
    }
}