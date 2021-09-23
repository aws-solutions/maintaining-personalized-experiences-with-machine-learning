package example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class Handler implements RequestHandler<UserData, UserData> {

    @Override
    public UserData handleRequest(UserData input, Context context) {
        UserData output = input;
        output.setGreeting("Hello there " + input.getName());
        return output;
    }
}
