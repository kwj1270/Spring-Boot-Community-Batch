package com.community.batch.jobs.inactive;

import com.community.batch.domain.User;

import org.springframework.batch.item.ItemProcessor;

public class InactiveItemProcessor implements ItemProcessor<User, User> {

    @Override
    public User process(User user) {
        return user.setInactive();
    }
}
