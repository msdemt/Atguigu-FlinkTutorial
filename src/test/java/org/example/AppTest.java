package org.example;

import com.google.gson.Gson;
import org.example.flink.wc.User;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test
    public void UserToStr(){
        User user = new User();
        user.setNsrsbh("15000120561127953X");
        user.setFjh("0");
        user.setJe("433773.9");
        user.setSe("21688.7");
        user.setKprq("2021-10-11 11:44:56");
        System.out.println(new Gson().toJson(user));
    }
}
