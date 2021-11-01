package org.example.userdemo.tracking;

import userdemo.Category;

import java.io.Serializable;

public class TrackingState implements Serializable {
    public int clothesViews = 0;
    public int foodViews = 0;
    public int toolsViews = 0;

    public int getViews(Category category) {
        switch (category) {
            case CLOTHES:
                return clothesViews;
            case FOOD:
                return foodViews;
            case TOOLS:
                return toolsViews;
            default:
                throw new IllegalArgumentException("Invalid category: " + category.toString());
        }
    }

    public void incrementViews(Category category) {
        switch (category) {
            case CLOTHES:
                clothesViews++;
                break;
            case FOOD:
                foodViews++;
                break;
            case TOOLS:
                toolsViews++;
                break;
            default:
                throw new IllegalArgumentException("Invalid category: " + category.toString());
        }
    }

    public Category mostViews() {
        Category[] categories = Category.values();
        Category mostViews = categories[0];
        for (Category category: categories) {
            if (getViews(category) > getViews(mostViews)) {
                mostViews = category;
            }
        }
        return mostViews;
    }
}
