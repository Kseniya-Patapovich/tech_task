package com.ifortex.bookservice.service.impl;

import com.ifortex.bookservice.dto.SearchCriteria;
import com.ifortex.bookservice.model.Book;
import com.ifortex.bookservice.service.BookService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// Attention! It is FORBIDDEN to make any changes in this file!
@Service
public class ESBookServiceImpl implements BookService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public Map<String, Long> getBooks() {
        String sql = "SELECT genre, COUNT(*) as count FROM books GROUP BY genre ORDER BY count DESC";

        List<Map<String, Object>> results = jdbcTemplate.query(sql, (ResultSet rs, int rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("genre", rs.getString("genre"));
            row.put("count", rs.getLong("count"));
            return row;
        });

        Map<String, Long> genreCountMap = new LinkedHashMap<>();
        results.forEach(row -> genreCountMap.put((String) row.get("genre"), (Long) row.get("count")));

        return genreCountMap;
    }

    @Override
    public List<Book> getAllByCriteria(SearchCriteria searchCriteria) {
        StringBuilder sql = new StringBuilder("SELECT * FROM books WHERE 1=1 ");
        List<Object> params = new ArrayList<>();

        if (searchCriteria.getTitle() != null && !searchCriteria.getTitle().isBlank()) {
            sql.append("AND title LIKE ? ");
            params.add("%" + searchCriteria.getTitle() + "%");
        }

        if (searchCriteria.getAuthor() != null && !searchCriteria.getAuthor().isBlank()) {
            sql.append("AND author LIKE ? ");
            params.add("%" + searchCriteria.getAuthor() + "%");
        }

        if (searchCriteria.getGenre() != null && !searchCriteria.getGenre().isBlank()) {
            sql.append("AND genre LIKE ? ");
            params.add("%" + searchCriteria.getGenre() + "%");
        }

        if (searchCriteria.getDescription() != null && !searchCriteria.getDescription().isBlank()) {
            sql.append("AND description LIKE ? ");
            params.add("%" + searchCriteria.getDescription() + "%");
        }

        if (searchCriteria.getYear() != null) {
            sql.append("AND YEAR(publication_date) = ? ");
            params.add(searchCriteria.getYear());
        }

        sql.append("ORDER BY publication_date DESC");

        return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
            Book book = new Book();
            book.setId(rs.getLong("id"));
            book.setTitle(rs.getString("title"));
            book.setDescription(rs.getString("description"));
            book.setAuthor(rs.getString("author"));
            book.setPublicationDate(rs.getTimestamp("publication_date").toLocalDateTime());
            book.setGenres(Set.of(rs.getString("genre").split(",")));
            return book;
        });
    }
}
