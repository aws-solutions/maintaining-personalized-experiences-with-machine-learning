package example;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HandlerTest {
    Handler handler;
    UserData userData;

    @BeforeEach
    void setUp() {
        handler = new Handler();
        userData = new UserData("AWS Solutions");
    }

    @Test
    void handleRequest() {
        UserData result = this.handler.handleRequest(userData, null);
        assert result.getGreeting().equals("Hello there AWS Solutions");
    }
}