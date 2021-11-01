package org.example.userdemo.order;

import userdemo.Category;

import java.io.Serializable;

public class OrderState implements Serializable {
    public int clothesOrders = 0;
    public int foodOrders = 0;
    public int toolsOrders = 0;

    public int getOrders(Category category) {
        switch (category) {
            case CLOTHES:
                return clothesOrders;
            case FOOD:
                return foodOrders;
            case TOOLS:
                return toolsOrders;
            default:
                throw new IllegalArgumentException("Invalid category: " + category.toString());
        }
    }

    public void increaseOrders(Category category, int amount) {
        switch (category) {
            case CLOTHES:
                clothesOrders += amount;
                break;
            case FOOD:
                foodOrders += amount;
                break;
            case TOOLS:
                toolsOrders += amount;
                break;
            default:
                throw new IllegalArgumentException("Invalid category: " + category.toString());
        }
    }

    public int totalOrders() {
        Category[] categories = Category.values();
        int totalOrders = 0;
        for (Category category: categories) {
            totalOrders += getOrders(category);
        }
        return totalOrders;
    }
}
