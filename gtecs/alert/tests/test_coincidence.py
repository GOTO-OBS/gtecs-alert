from gtecs.alert import handler, notices

n1 = notices.Notice.from_file(
    "/home/martin/Dropbox/GOTO/0_code/gtecs-alert/gtecs/alert/data/test_notices/Fermi/730284696-GBM_FIN_POS-GRB_240222A.json"
)
n2 = notices.Notice.from_file(
    "/home/martin/Dropbox/GOTO/0_code/gtecs-alert/gtecs/alert/data/test_notices/Swift/1216804-BAT_GRB_POS-GRB_240222A.json"
)
n3 = notices.Notice.from_file(
    "/home/martin/Dropbox/GOTO/0_code/gtecs-alert/gtecs/alert/data/test_notices/GECAM/297-FLT-GRB_240222A.json"
)
n3.strategy_dict["min_tile_prob"] = 0.001  # To ensure tiles are actually selected

print('~~~~~~')
print(n1)
handler.handle_notice(n1, send_messages=False)
print('~~~~~~')
print(n2)
handler.handle_notice(n2, send_messages=False)
print('~~~~~~')
print(n3)
handler.handle_notice(n3, send_messages=False)
print('~~~~~~')
