import{_ as u}from"./ChannelSubscribe.vue_vue_type_script_setup_true_lang.46ac8ba4.js";import h from"./Monitor.c2c88fd4.js";import{a as b,x as f,v,b as y,o as x,h as C,j as e,w as n,m as S,u as g,r as c,i as a}from"./entry.86b9e19f.js";const w=a("i",{class:"pi pi-angle-double-down mr-2"},null,-1),T=a("span",{class:"text-lg"},"Subscribe",-1),V=a("i",{class:"pi pi-eye mr-2"},null,-1),B=a("span",{class:"text-lg"},"Watch",-1),N=b({__name:"ChannelActions",props:{channel:{type:String,default:()=>""},type:{type:String,default:()=>""}},setup(i){const t=i;f(()=>{o.value=0});const o=v(0),{isStreamReady:r}=y();return(I,s)=>{const p=u,l=c("TabPanel"),_=h,d=c("TabView");return x(),C("div",{class:S(["col-12 md:col-12",g(r)?"":"div-disabled"])},[e(d,{activeIndex:o.value,"onUpdate:activeIndex":s[0]||(s[0]=m=>o.value=m)},{default:n(()=>[e(l,{ref:"tab1"},{header:n(()=>[w,T]),default:n(()=>[e(p,{channel:t.channel,type:t.type},null,8,["channel","type"])]),_:1},512),e(l,null,{header:n(()=>[V,B]),default:n(()=>[e(_,{channel:t.channel,type:t.type},null,8,["channel","type"])]),_:1})]),_:1},8,["activeIndex"])],2)}}});export{N as _};
