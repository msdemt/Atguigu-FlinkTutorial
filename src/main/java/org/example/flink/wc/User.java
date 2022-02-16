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
    private String slv;
    private String kprq;

    public User() {
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

    public String getSlv() {
        return slv;
    }

    public void setSlv(String slv) {
        this.slv = slv;
    }

    public String getKprq() {
        return kprq;
    }

    public void setKprq(String kprq) {
        this.kprq = kprq;
    }

    @Override
    public String toString() {
        return "User{" +
                "nsrsbh='" + nsrsbh + '\'' +
                ", fjh='" + fjh + '\'' +
                ", je='" + je + '\'' +
                ", se='" + se + '\'' +
                ", slv='" + slv + '\'' +
                ", kprq='" + kprq + '\'' +
                '}';
    }
}
