package com.ifortex.bookservice.service.impl;

import com.ifortex.bookservice.model.Member;
import com.ifortex.bookservice.service.MemberService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

// Attention! It is FORBIDDEN to make any changes in this file!
@Service
public class ESMemberServiceImpl implements MemberService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public Member findMember() {
        String sql = """
                SELECT m.id, m.name, m.membership_date
                FROM members m
                JOIN member_books mb ON m.id = mb.member_id
                JOIN books b ON mb.book_id = b.id
                WHERE b.genre LIKE '%Romance%'
                ORDER BY b.publication_date ASC, m.membership_date DESC
                LIMIT 1
                """;

        return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
            Member member = new Member();
            member.setId(rs.getLong("id"));
            member.setName(rs.getString("name"));
            member.setMembershipDate(rs.getTimestamp("membership_date").toLocalDateTime());
            return member;
        });
    }

    @Override
    public List<Member> findMembers() {
        String sql = """
                SELECT m.id, m.name, m.membership_date
                FROM members m
                LEFT JOIN member_books mb ON m.id = mb.member_id
                WHERE YEAR(m.membership_date) = 2023 AND mb.member_id IS NULL
                """;

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Member member = new Member();
            member.setId(rs.getLong("id"));
            member.setName(rs.getString("name"));
            member.setMembershipDate(rs.getTimestamp("membership_date").toLocalDateTime());
            return member;
        });
    }
}
