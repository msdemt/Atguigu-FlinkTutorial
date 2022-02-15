package org.example.flink.wc;

/**
 * @Description: 纳税人实体
 * @Author: hekai
 * @Date: 2022-02-15 10:29
 */
public class User {

    private String nsrsbh;
    private String fjh;
    private String je;
    private String se;
    private String kprq;

    public User() {
    }

    public User(String nsrsbh, String fjh, String je, String se) {
        this.nsrsbh = nsrsbh;
        this.fjh = fjh;
        this.je = je;
        this.se = se;
    }

    public String getNsrsbh() {
        return nsrsbh;
    }

    public void setNsrsbh(String nsrsbh) {
        this.nsrsbh = nsrsbh;
    }

    public String getFjh() {
        return fjh;
    }

    public void setFjh(String fjh) {
        this.fjh = fjh;
    }

    public String getJe() {
        return je;
    }

    public void setJe(String je) {
        this.je = je;
    }

    public String getSe() {
        return se;
    }

    public void setSe(String se) {
        this.se = se;
    }

    public String getKprq() {
        return kprq;
    }

    public void setKprq(String kprq) {
        this.kprq = kprq;
    }
}
